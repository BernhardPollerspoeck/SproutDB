using System.Text.Json;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Routing;
using SproutDB.Core.Auth;
using SproutDB.Core.Execution;
using SproutDB.Core.Parsing;

namespace SproutDB.Core.Server;

internal static class SproutEndpoints
{
    private static readonly JsonSerializerOptions JsonOptions = new()
    {
        PropertyNamingPolicy = JsonNamingPolicy.SnakeCaseLower,
    };

    // Auth query keywords at the start of a query
    private static readonly HashSet<string> AuthQueryKeywords = new(StringComparer.OrdinalIgnoreCase)
    {
        "grant", "revoke", "restrict", "unrestrict", "rotate",
    };

    internal static void Map(IEndpointRouteBuilder endpoints)
    {
        endpoints.MapPost("/sproutdb/query", HandleQuery);
    }

    private static async Task<IResult> HandleQuery(HttpContext context, SproutEngine engine)
    {
        // 1. Read query body
        using var reader = new StreamReader(context.Request.Body);
        var query = await reader.ReadToEndAsync();

        if (string.IsNullOrWhiteSpace(query))
            return Results.BadRequest(new { error = "Request body must contain a query" });

        // 2. Detect if this is an auth query (doesn't need database header)
        var isAuthQuery = IsAuthQueryText(query);

        // 3. Auth validation (if auth is enabled)
        if (engine.AuthService is not null)
        {
            var apiKeyHeader = context.Request.Headers["X-SproutDB-ApiKey"].ToString();

            if (string.IsNullOrWhiteSpace(apiKeyHeader))
            {
                var errorResponse = ResponseHelper.Error(query, ErrorCodes.AUTH_REQUIRED,
                    "missing required header: X-SproutDB-ApiKey");
                return Results.Json(new[] { errorResponse }, JsonOptions, statusCode: StatusCodes.Status401Unauthorized);
            }

            if (isAuthQuery)
            {
                // Auth queries require MasterKey or admin on the target DB
                if (!engine.AuthService.IsMasterKey(apiKeyHeader))
                {
                    // Check if it's a grant/revoke/restrict/unrestrict — could be done by an admin
                    var key = engine.AuthService.ValidateKey(apiKeyHeader);
                    if (key is null)
                    {
                        var errorResponse = ResponseHelper.Error(query, ErrorCodes.AUTH_INVALID,
                            "invalid api key");
                        return Results.Json(new[] { errorResponse }, JsonOptions, statusCode: StatusCodes.Status401Unauthorized);
                    }

                    // Only master key can create/purge/rotate keys
                    if (IsKeyManagementQuery(query))
                    {
                        var errorResponse = ResponseHelper.Error(query, ErrorCodes.PERMISSION_DENIED,
                            "key management requires master key");
                        return Results.Json(new[] { errorResponse }, JsonOptions, statusCode: StatusCodes.Status403Forbidden);
                    }

                    // For grant/revoke/restrict/unrestrict: check if key has admin on the target DB
                    var targetDb = ExtractTargetDatabase(query);
                    if (targetDb is not null && key.Permissions.TryGetValue(targetDb, out var role)
                        && string.Equals(role, "admin", StringComparison.OrdinalIgnoreCase))
                    {
                        // Admin on target DB can manage permissions
                    }
                    else
                    {
                        var errorResponse = ResponseHelper.Error(query, ErrorCodes.PERMISSION_DENIED,
                            "requires master key or admin on target database");
                        return Results.Json(new[] { errorResponse }, JsonOptions, statusCode: StatusCodes.Status403Forbidden);
                    }
                }
            }
            else
            {
                // Normal queries: validate key and check permissions
                if (!engine.AuthService.IsMasterKey(apiKeyHeader))
                {
                    var key = engine.AuthService.ValidateKey(apiKeyHeader);
                    if (key is null)
                    {
                        var errorResponse = ResponseHelper.Error(query, ErrorCodes.AUTH_INVALID,
                            "invalid api key");
                        return Results.Json(new[] { errorResponse }, JsonOptions, statusCode: StatusCodes.Status401Unauthorized);
                    }

                    // Parse to check permissions for all queries
                    var parseResults = QueryParser.ParseMulti(query);
                    var dbHeader = context.Request.Headers["X-SproutDB-Database"].ToString();
                    if (!string.IsNullOrWhiteSpace(dbHeader))
                    {
                        foreach (var parseResult in parseResults)
                        {
                            if (!parseResult.Success || parseResult.Query is null)
                                continue;

                            // For transactions, check each inner query
                            if (parseResult.Query is TransactionQuery txq)
                            {
                                foreach (var innerQuery in txq.Queries)
                                {
                                    var permError = engine.AuthService.CheckPermission(key, innerQuery, dbHeader.ToLowerInvariant());
                                    if (permError is not null)
                                        return Results.Json(new[] { permError }, JsonOptions, statusCode: StatusCodes.Status403Forbidden);
                                }
                            }
                            else
                            {
                                var permError = engine.AuthService.CheckPermission(key, parseResult.Query, dbHeader.ToLowerInvariant());
                                if (permError is not null)
                                    return Results.Json(new[] { permError }, JsonOptions, statusCode: StatusCodes.Status403Forbidden);
                            }
                        }
                    }
                }
            }
        }

        // 4. Auth queries don't need database header — use "_system"
        if (isAuthQuery)
        {
            var responses = engine.Execute(query, "_system");
            return Results.Json(responses, JsonOptions, statusCode: StatusCodes.Status200OK);
        }

        // 5. Normal queries require database header
        if (!context.Request.Headers.TryGetValue("X-SproutDB-Database", out var dbHeaderValue)
            || string.IsNullOrWhiteSpace(dbHeaderValue))
        {
            return Results.BadRequest(new { error = "Missing required header: X-SproutDB-Database" });
        }

        var database = dbHeaderValue.ToString();

        var normalResponses = engine.Execute(query, database);
        return Results.Json(normalResponses, JsonOptions, statusCode: StatusCodes.Status200OK);
    }

    /// <summary>
    /// Detects auth query keywords from the raw query text (lightweight, no full parse).
    /// </summary>
    private static bool IsAuthQueryText(string query)
    {
        var trimmed = query.AsSpan().TrimStart();

        // Check for "create apikey" and "purge apikey"
        if (trimmed.StartsWith("create", StringComparison.OrdinalIgnoreCase)
            || trimmed.StartsWith("CREATE", StringComparison.OrdinalIgnoreCase))
        {
            var rest = trimmed[6..].TrimStart();
            if (rest.StartsWith("apikey", StringComparison.OrdinalIgnoreCase))
                return true;
            return false;
        }

        if (trimmed.StartsWith("purge", StringComparison.OrdinalIgnoreCase))
        {
            var rest = trimmed[5..].TrimStart();
            if (rest.StartsWith("apikey", StringComparison.OrdinalIgnoreCase))
                return true;
            return false;
        }

        // Check first word against auth keywords
        var spaceIdx = trimmed.IndexOfAny([' ', '\t', '\n', '\r']);
        var firstWord = spaceIdx > 0 ? trimmed[..spaceIdx] : trimmed;

        foreach (var keyword in AuthQueryKeywords)
        {
            if (firstWord.Equals(keyword, StringComparison.OrdinalIgnoreCase))
                return true;
        }

        return false;
    }

    private static bool IsKeyManagementQuery(string query)
    {
        var trimmed = query.AsSpan().TrimStart();

        if (trimmed.StartsWith("create", StringComparison.OrdinalIgnoreCase))
        {
            var rest = trimmed[6..].TrimStart();
            return rest.StartsWith("apikey", StringComparison.OrdinalIgnoreCase);
        }

        if (trimmed.StartsWith("purge", StringComparison.OrdinalIgnoreCase))
        {
            var rest = trimmed[5..].TrimStart();
            return rest.StartsWith("apikey", StringComparison.OrdinalIgnoreCase);
        }

        if (trimmed.StartsWith("rotate", StringComparison.OrdinalIgnoreCase))
            return true;

        return false;
    }

    /// <summary>
    /// Extracts the target database name from grant/revoke/restrict/unrestrict queries.
    /// Used for authorization check (admin on target DB can manage permissions).
    /// </summary>
    private static string? ExtractTargetDatabase(string query)
    {
        // Parse the query to extract database
        var parseResult = QueryParser.Parse(query);
        if (!parseResult.Success || parseResult.Query is null)
            return null;

        return parseResult.Query switch
        {
            GrantQuery q => q.Database,
            RevokeQuery q => q.Database,
            RestrictQuery q => q.Database,
            UnrestrictQuery q => q.Database,
            _ => null,
        };
    }

}

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
        // 1. Read query body: text/plain = the query itself,
        //    application/json = {"query": "...", "parameters": {"name": value, ...}}
        using var reader = new StreamReader(context.Request.Body);
        var body = await reader.ReadToEndAsync();

        var query = body;
        if (context.Request.HasJsonContentType())
        {
            if (!TryReadJsonRequest(body, out var jsonQuery, out var parameters, out var jsonError))
                return Results.BadRequest(new { error = jsonError });

            // Bind before anything else looks at the query (auth checks included)
            if (!QueryParameters.TryBind(jsonQuery, parameters, out var bound, out var bindError))
            {
                var error = bindError ?? ResponseHelper.Error(jsonQuery, ErrorCodes.PARAMETER_ERROR, "invalid parameters");
                return Results.Json(new[] { error }, JsonOptions, statusCode: StatusCodes.Status200OK);
            }
            query = bound;
        }

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
        // Async execution: request threads don't block while writes wait in the writer
        // queue. No cancellation token — a client disconnect must not skip a write.
        if (isAuthQuery)
        {
            var responses = await engine.ExecuteAsync(query, "_system");
            return Results.Json(responses, JsonOptions, statusCode: StatusCodes.Status200OK);
        }

        // 5. Normal queries require database header
        if (!context.Request.Headers.TryGetValue("X-SproutDB-Database", out var dbHeaderValue)
            || string.IsNullOrWhiteSpace(dbHeaderValue))
        {
            return Results.BadRequest(new { error = "Missing required header: X-SproutDB-Database" });
        }

        var database = dbHeaderValue.ToString();

        var normalResponses = await engine.ExecuteAsync(query, database);
        return Results.Json(normalResponses, JsonOptions, statusCode: StatusCodes.Status200OK);
    }

    /// <summary>
    /// Parses a JSON request <c>{"query": "...", "parameters": {...}}</c>. Parameter
    /// values: string, number, true/false, null or an array of those.
    /// </summary>
    private static bool TryReadJsonRequest(string body, out string query,
        out Dictionary<string, object?>? parameters, out string error)
    {
        query = "";
        parameters = null;
        error = "";

        try
        {
            using var doc = JsonDocument.Parse(body);
            var root = doc.RootElement;
            if (root.ValueKind != JsonValueKind.Object
                || !root.TryGetProperty("query", out var queryElement)
                || queryElement.ValueKind != JsonValueKind.String)
            {
                error = "JSON body must be an object with a string property 'query'";
                return false;
            }
            query = queryElement.GetString() ?? "";

            if (root.TryGetProperty("parameters", out var paramsElement) && paramsElement.ValueKind != JsonValueKind.Null)
            {
                if (paramsElement.ValueKind != JsonValueKind.Object)
                {
                    error = "'parameters' must be a JSON object";
                    return false;
                }

                parameters = new Dictionary<string, object?>();
                foreach (var prop in paramsElement.EnumerateObject())
                {
                    if (!TryConvertJsonValue(prop.Value, out var value))
                    {
                        error = $"parameter '{prop.Name}': objects are not supported as values";
                        return false;
                    }
                    parameters[prop.Name] = value;
                }
            }

            return true;
        }
        catch (JsonException ex)
        {
            error = $"invalid JSON: {ex.Message}";
            return false;
        }
    }

    private static bool TryConvertJsonValue(JsonElement element, out object? value)
    {
        value = null;
        switch (element.ValueKind)
        {
            case JsonValueKind.String:
                value = element.GetString();
                return true;
            case JsonValueKind.True:
                value = true;
                return true;
            case JsonValueKind.False:
                value = false;
                return true;
            case JsonValueKind.Null:
                return true;
            case JsonValueKind.Number:
                if (element.TryGetInt64(out var l)) value = l;
                else if (element.TryGetUInt64(out var ul)) value = ul;
                else if (element.TryGetDecimal(out var dec)) value = dec;
                else value = element.GetDouble();
                return true;
            case JsonValueKind.Array:
                var list = new List<object?>();
                foreach (var item in element.EnumerateArray())
                {
                    if (!TryConvertJsonValue(item, out var itemValue))
                        return false;
                    list.Add(itemValue);
                }
                value = list;
                return true;
            default:
                return false;
        }
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

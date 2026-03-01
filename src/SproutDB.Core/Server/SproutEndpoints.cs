using System.Text.Json;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Routing;
using SproutDB.Core.Parsing;

namespace SproutDB.Core.Server;

internal static class SproutEndpoints
{
    private static readonly JsonSerializerOptions JsonOptions = new()
    {
        PropertyNamingPolicy = JsonNamingPolicy.SnakeCaseLower,
    };

    internal static void Map(IEndpointRouteBuilder endpoints)
    {
        endpoints.MapPost("/sproutdb/query", HandleQuery);
    }

    private static async Task<IResult> HandleQuery(HttpContext context, SproutEngine engine)
    {
        if (!context.Request.Headers.TryGetValue("X-SproutDB-Database", out var dbHeader)
            || string.IsNullOrWhiteSpace(dbHeader))
        {
            return Results.BadRequest(new { error = "Missing required header: X-SproutDB-Database" });
        }

        var database = dbHeader.ToString();

        using var reader = new StreamReader(context.Request.Body);
        var query = await reader.ReadToEndAsync();

        if (string.IsNullOrWhiteSpace(query))
        {
            return Results.BadRequest(new { error = "Request body must contain a query" });
        }

        var response = engine.Execute(query, database);
        var statusCode = MapStatusCode(response);

        return Results.Json(response, JsonOptions, statusCode: statusCode);
    }

    private static int MapStatusCode(SproutResponse response)
    {
        if (response.Errors is null || response.Errors.Count == 0)
            return StatusCodes.Status200OK;

        var code = response.Errors[0].Code;

        return code switch
        {
            ErrorCodes.UNKNOWN_DATABASE
                or ErrorCodes.UNKNOWN_TABLE
                or ErrorCodes.UNKNOWN_COLUMN
                or ErrorCodes.INDEX_NOT_FOUND => StatusCodes.Status404NotFound,

            ErrorCodes.DATABASE_EXISTS
                or ErrorCodes.TABLE_EXISTS
                or ErrorCodes.INDEX_EXISTS => StatusCodes.Status409Conflict,

            ErrorCodes.PROTECTED_NAME => StatusCodes.Status403Forbidden,

            _ => StatusCodes.Status400BadRequest,
        };
    }
}

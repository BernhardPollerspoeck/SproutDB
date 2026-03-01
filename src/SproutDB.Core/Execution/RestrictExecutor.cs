using SproutDB.Core.Auth;
using SproutDB.Core.Parsing;
using SproutDB.Core.Storage;

namespace SproutDB.Core.Execution;

internal static class RestrictExecutor
{
    public static SproutResponse Execute(
        string query,
        RestrictQuery q,
        TableHandle apiRestrictionsTable,
        SproutAuthService authService,
        int bulkLimit)
    {
        if (!authService.KeyExists(q.KeyName))
            return ResponseHelper.Error(query, ErrorCodes.KEY_NOT_FOUND,
                $"api key '{q.KeyName}' not found");

        // Upsert restriction
        var upsertQuery = $"upsert _api_restrictions {{key_name: '{Escape(q.KeyName)}', database: '{Escape(q.Database)}', table: '{Escape(q.Table)}', role: '{Escape(q.Role)}'}}";
        var parseResult = QueryParser.Parse(upsertQuery);
        if (!parseResult.Success || parseResult.Query is not UpsertQuery uq)
            return ResponseHelper.Error(query, ErrorCodes.SYNTAX_ERROR, "internal error restricting access");

        var result = UpsertExecutor.Execute(upsertQuery, apiRestrictionsTable, uq, bulkLimit);
        if (result.Errors is not null)
            return result;

        authService.OnRestricted(q.KeyName, q.Database, q.Table, q.Role);

        return new SproutResponse
        {
            Operation = SproutOperation.Restrict,
            Affected = 1,
        };
    }

    private static string Escape(string value) => value.Replace("'", "\\'");
}

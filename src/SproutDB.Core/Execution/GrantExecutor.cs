using SproutDB.Core.Auth;
using SproutDB.Core.Parsing;
using SproutDB.Core.Storage;

namespace SproutDB.Core.Execution;

internal static class GrantExecutor
{
    public static SproutResponse Execute(
        string query,
        GrantQuery q,
        TableHandle apiPermissionsTable,
        SproutAuthService authService,
        int bulkLimit)
    {
        if (!authService.KeyExists(q.KeyName))
            return ResponseHelper.Error(query, ErrorCodes.KEY_NOT_FOUND,
                $"api key '{q.KeyName}' not found");

        // Upsert permission (key_name + database is the logical key)
        var upsertQuery = $"upsert _api_permissions {{key_name: '{Escape(q.KeyName)}', database: '{Escape(q.Database)}', role: '{Escape(q.Role)}'}}";
        var parseResult = QueryParser.Parse(upsertQuery);
        if (!parseResult.Success || parseResult.Query is not UpsertQuery uq)
            return ResponseHelper.Error(query, ErrorCodes.SYNTAX_ERROR, "internal error granting permission");

        var result = UpsertExecutor.Execute(upsertQuery, apiPermissionsTable, uq, bulkLimit);
        if (result.Errors is not null)
            return result;

        authService.OnGranted(q.KeyName, q.Database, q.Role);

        return new SproutResponse
        {
            Operation = SproutOperation.Grant,
            Affected = 1,
        };
    }

    private static string Escape(string value) => value.Replace("'", "\\'");
}

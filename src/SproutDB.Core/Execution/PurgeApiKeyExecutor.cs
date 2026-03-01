using SproutDB.Core.Auth;
using SproutDB.Core.Parsing;
using SproutDB.Core.Storage;

namespace SproutDB.Core.Execution;

internal static class PurgeApiKeyExecutor
{
    public static SproutResponse Execute(
        string query,
        PurgeApiKeyQuery q,
        TableHandle apiKeysTable,
        TableHandle apiPermissionsTable,
        TableHandle apiRestrictionsTable,
        SproutAuthService authService,
        int bulkLimit)
    {
        if (!authService.KeyExists(q.Name))
            return ResponseHelper.Error(query, ErrorCodes.KEY_NOT_FOUND,
                $"api key '{q.Name}' not found");

        // Delete from _api_keys
        DeleteByField(apiKeysTable, "name", q.Name, bulkLimit);

        // Delete from _api_permissions
        DeleteByField(apiPermissionsTable, "key_name", q.Name, bulkLimit);

        // Delete from _api_restrictions
        DeleteByField(apiRestrictionsTable, "key_name", q.Name, bulkLimit);

        authService.OnKeyPurged(q.Name);

        return new SproutResponse
        {
            Operation = SproutOperation.PurgeApiKey,
            Affected = 1,
        };
    }

    private static void DeleteByField(TableHandle table, string field, string value, int bulkLimit)
    {
        var deleteQuery = $"delete _placeholder where {field} = '{Escape(value)}'";
        var parseResult = QueryParser.Parse(deleteQuery);
        if (parseResult.Success && parseResult.Query is DeleteQuery dq)
            DeleteExecutor.Execute(deleteQuery, table, dq);
    }

    private static string Escape(string value) => value.Replace("'", "\\'");
}

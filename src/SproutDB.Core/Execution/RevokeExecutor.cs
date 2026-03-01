using SproutDB.Core.Auth;
using SproutDB.Core.Parsing;
using SproutDB.Core.Storage;

namespace SproutDB.Core.Execution;

internal static class RevokeExecutor
{
    public static SproutResponse Execute(
        string query,
        RevokeQuery q,
        TableHandle apiPermissionsTable,
        TableHandle apiRestrictionsTable,
        SproutAuthService authService,
        int bulkLimit)
    {
        if (!authService.KeyExists(q.KeyName))
            return ResponseHelper.Error(query, ErrorCodes.KEY_NOT_FOUND,
                $"api key '{q.KeyName}' not found");

        // Delete permission for this database
        var deletePermQuery = $"delete _placeholder where key_name = '{Escape(q.KeyName)}' and database = '{Escape(q.Database)}'";
        var parseResult = QueryParser.Parse(deletePermQuery);
        if (parseResult.Success && parseResult.Query is DeleteQuery dq)
            DeleteExecutor.Execute(deletePermQuery, apiPermissionsTable, dq);

        // Delete all restrictions for this database
        var deleteRestQuery = $"delete _placeholder where key_name = '{Escape(q.KeyName)}' and database = '{Escape(q.Database)}'";
        var restParseResult = QueryParser.Parse(deleteRestQuery);
        if (restParseResult.Success && restParseResult.Query is DeleteQuery rdq)
            DeleteExecutor.Execute(deleteRestQuery, apiRestrictionsTable, rdq);

        authService.OnRevoked(q.KeyName, q.Database);

        return new SproutResponse
        {
            Operation = SproutOperation.Revoke,
            Affected = 1,
        };
    }

    private static string Escape(string value) => value.Replace("'", "\\'");
}

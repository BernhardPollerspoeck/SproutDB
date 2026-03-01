using SproutDB.Core.Auth;
using SproutDB.Core.Parsing;
using SproutDB.Core.Storage;

namespace SproutDB.Core.Execution;

internal static class UnrestrictExecutor
{
    public static SproutResponse Execute(
        string query,
        UnrestrictQuery q,
        TableHandle apiRestrictionsTable,
        SproutAuthService authService,
        int bulkLimit)
    {
        if (!authService.KeyExists(q.KeyName))
            return ResponseHelper.Error(query, ErrorCodes.KEY_NOT_FOUND,
                $"api key '{q.KeyName}' not found");

        // Delete the specific restriction
        var deleteQuery = $"delete _placeholder where key_name = '{Escape(q.KeyName)}' and database = '{Escape(q.Database)}' and table = '{Escape(q.Table)}'";
        var parseResult = QueryParser.Parse(deleteQuery);
        if (parseResult.Success && parseResult.Query is DeleteQuery dq)
            DeleteExecutor.Execute(deleteQuery, apiRestrictionsTable, dq);

        authService.OnUnrestricted(q.KeyName, q.Database, q.Table);

        return new SproutResponse
        {
            Operation = SproutOperation.Unrestrict,
            Affected = 1,
        };
    }

    private static string Escape(string value) => value.Replace("'", "\\'");
}

using SproutDB.Core.Parsing;
using SproutDB.Core.Storage;

namespace SproutDB.Core.Execution;

internal static class PurgeColumnExecutor
{
    public static SproutResponse Execute(string query, TableHandle table, PurgeColumnQuery q)
    {
        // Column "_id" is reserved (parser blocks this, but guard here too)
        if (q.Column == "_id")
            return ResponseHelper.Error(query, ErrorCodes.SYNTAX_ERROR,
                ErrorMessages.RESERVED_COLUMN_NAME_ID);

        // Idempotent: column doesn't exist → silent OK
        if (!table.HasColumn(q.Column))
        {
            return new SproutResponse
            {
                Operation = SproutOperation.PurgeColumn,
                Schema = new SchemaInfo
                {
                    Table = q.Table,
                    Columns = ResponseHelper.BuildColumnInfoList(table.Schema),
                },
            };
        }

        table.RemoveColumn(q.Column);

        return new SproutResponse
        {
            Operation = SproutOperation.PurgeColumn,
            Schema = new SchemaInfo
            {
                Table = q.Table,
                Columns = ResponseHelper.BuildColumnInfoList(table.Schema),
            },
        };
    }
}

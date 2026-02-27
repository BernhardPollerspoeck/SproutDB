using SproutDB.Core.Parsing;
using SproutDB.Core.Storage;

namespace SproutDB.Core.Execution;

internal static class RenameColumnExecutor
{
    public static SproutResponse Execute(string query, TableHandle table, RenameColumnQuery q)
    {
        // Idempotent: old doesn't exist but new does → already renamed
        if (!table.HasColumn(q.OldColumn) && table.HasColumn(q.NewColumn))
        {
            return SuccessResponse(q.Table, table.Schema);
        }

        // Old column must exist
        if (!table.HasColumn(q.OldColumn))
        {
            return ResponseHelper.Error(query, ErrorCodes.UNKNOWN_COLUMN,
                $"column '{q.OldColumn}' does not exist");
        }

        // New column must not already exist (unless same name = no-op)
        if (q.OldColumn == q.NewColumn)
            return SuccessResponse(q.Table, table.Schema);

        if (table.HasColumn(q.NewColumn))
        {
            return ResponseHelper.Error(query, ErrorCodes.SYNTAX_ERROR,
                $"column '{q.NewColumn}' already exists");
        }

        table.RenameColumn(q.OldColumn, q.NewColumn);

        return SuccessResponse(q.Table, table.Schema);
    }

    private static SproutResponse SuccessResponse(string tableName, TableSchema schema)
    {
        return new SproutResponse
        {
            Operation = SproutOperation.RenameColumn,
            Schema = new SchemaInfo
            {
                Table = tableName,
                Columns = ResponseHelper.BuildColumnInfoList(schema),
            },
        };
    }
}

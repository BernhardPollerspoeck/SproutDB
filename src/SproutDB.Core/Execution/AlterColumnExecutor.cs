using SproutDB.Core.Parsing;
using SproutDB.Core.Storage;

namespace SproutDB.Core.Execution;

internal static class AlterColumnExecutor
{
    public static SproutResponse Execute(string query, TableHandle table, AlterColumnQuery q)
    {
        var existing = table.Schema.Columns.Find(c => c.Name == q.Column);
        if (existing is null)
            return ResponseHelper.Error(query, ErrorCodes.UNKNOWN_COLUMN,
                $"column '{q.Column}' does not exist");

        // Only string columns can be altered
        ColumnTypes.TryParse(existing.Type, out var colType);
        if (colType != ColumnType.String)
            return ResponseHelper.Error(query, ErrorCodes.SYNTAX_ERROR,
                $"column '{q.Column}' is type '{existing.Type}', only string columns can be altered");

        // Same size → idempotent no-op
        if (existing.Size == q.NewSize)
            return SuccessResponse(q.Table, table.Schema);

        table.RebuildColumn(q.Column, q.NewSize);

        return SuccessResponse(q.Table, table.Schema);
    }

    private static SproutResponse SuccessResponse(string tableName, TableSchema schema)
    {
        return new SproutResponse
        {
            Operation = SproutOperation.AlterColumn,
            Schema = new SchemaInfo
            {
                Table = tableName,
                Columns = ResponseHelper.BuildColumnInfoList(schema),
            },
        };
    }
}

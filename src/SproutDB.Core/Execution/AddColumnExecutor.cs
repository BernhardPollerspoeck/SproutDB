using SproutDB.Core.Parsing;
using SproutDB.Core.Storage;

namespace SproutDB.Core.Execution;

internal static class AddColumnExecutor
{
    public static SproutResponse Execute(string query, TableHandle table, AddColumnQuery q)
    {
        // Check if column already exists
        var existing = table.Schema.Columns.Find(c => c.Name == q.Column.Name);
        if (existing is not null)
            return HandleExistingColumn(query, table, q, existing);

        // New column
        var entry = new ColumnSchemaEntry
        {
            Name = q.Column.Name,
            Type = ColumnTypes.GetName(q.Column.Type),
            Size = q.Column.Size,
            EntrySize = q.Column.EntrySize,
            Nullable = q.Column.IsNullable,
            Default = q.Column.Default,
            Strict = q.Column.Strict,
        };

        table.AddColumn(entry);

        // Backfill defaults for existing rows
        if (q.Column.Default is not null)
        {
            var colHandle = table.GetColumn(q.Column.Name);
            table.Index.ForEachUsed((_, place) =>
            {
                colHandle.WriteValue(place, q.Column.Default);
            });
        }

        table.SaveSchema();
        return SuccessResponse(q.Table, table.Schema);
    }

    private static SproutResponse HandleExistingColumn(
        string query, TableHandle table, AddColumnQuery q, ColumnSchemaEntry existing)
    {
        ColumnTypes.TryParse(existing.Type, out var existingType);
        var newType = q.Column.Type;

        // Same type → silent OK (idempotent)
        if (existingType == newType)
            return SuccessResponse(q.Table, table.Schema);

        // Strict violation
        if (existing.Strict)
            return ResponseHelper.Error(query, ErrorCodes.STRICT_VIOLATION,
                $"column '{q.Column.Name}' is strict, type expansion from '{existing.Type}' to '{ColumnTypes.GetName(newType)}' not allowed");

        // Type narrowing
        if (!ColumnTypes.CanExpand(existingType, newType))
            return ResponseHelper.Error(query, ErrorCodes.TYPE_NARROWING,
                $"cannot narrow type from '{existing.Type}' to '{ColumnTypes.GetName(newType)}'");

        // Type expansion: update schema
        existing.Type = ColumnTypes.GetName(newType);
        existing.Size = q.Column.Size;
        existing.EntrySize = q.Column.EntrySize;

        table.SaveSchema();
        return SuccessResponse(q.Table, table.Schema);
    }

    private static SproutResponse SuccessResponse(string table, TableSchema schema)
    {
        return new SproutResponse
        {
            Operation = SproutOperation.AddColumn,
            Schema = new SchemaInfo
            {
                Table = table,
                Columns = ResponseHelper.BuildColumnInfoList(schema),
            },
        };
    }
}

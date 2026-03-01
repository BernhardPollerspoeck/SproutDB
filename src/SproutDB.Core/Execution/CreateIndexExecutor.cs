using SproutDB.Core.Parsing;
using SproutDB.Core.Storage;

namespace SproutDB.Core.Execution;

internal static class CreateIndexExecutor
{
    public static SproutResponse Execute(string query, TableHandle table, CreateIndexQuery q)
    {
        // Column must exist
        if (!table.HasColumn(q.Column))
            return ResponseHelper.Error(query, ErrorCodes.UNKNOWN_COLUMN,
                $"column '{q.Column}' does not exist");

        // Index must not already exist
        if (table.HasBTree(q.Column))
            return ResponseHelper.Error(query, ErrorCodes.INDEX_EXISTS,
                $"index on '{q.Column}' already exists");

        var colHandle = table.GetColumn(q.Column);
        var schema = colHandle.Schema;
        ColumnTypes.TryParse(schema.Type, out var colType);

        var btreePath = Path.Combine(table.TablePath, $"{q.Column}.btree");

        var btree = BTreeHandle.BuildFromColumn(btreePath, colHandle, table.Index,
            colType, schema.Size);

        table.AddBTree(q.Column, btree);

        return new SproutResponse
        {
            Operation = SproutOperation.CreateIndex,
            Affected = 1,
        };
    }
}

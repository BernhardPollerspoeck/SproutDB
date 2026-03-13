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

        // Blob/Array columns cannot be indexed
        if (colType == ColumnType.Blob)
            return ResponseHelper.Error(query, ErrorCodes.TYPE_MISMATCH,
                "cannot create index on blob column");
        if (colType == ColumnType.Array)
            return ResponseHelper.Error(query, ErrorCodes.TYPE_MISMATCH,
                "cannot create index on array column");

        // Unique: check existing data for duplicates before building the index
        if (q.Unique)
        {
            var seen = new HashSet<ByteKey>();
            bool hasDuplicate = false;

            table.Index.ForEachUsed((_, place) =>
            {
                if (hasDuplicate) return;
                if (colHandle.IsNullAtPlace(place)) return; // nulls are always allowed

                var val = colHandle.ReadValue(place);
                if (val is null) return;

                var encoded = colHandle.EncodeValueToBytes(val.ToString() ?? "");
                if (!seen.Add(new ByteKey(encoded)))
                    hasDuplicate = true;
            });

            if (hasDuplicate)
                return ResponseHelper.Error(query, ErrorCodes.UNIQUE_VIOLATION,
                    $"cannot create unique index on '{q.Column}': column contains duplicate values");
        }

        var btreePath = Path.Combine(table.TablePath, $"{q.Column}.btree");

        var btree = BTreeHandle.BuildFromColumn(btreePath, colHandle, table.Index,
            colType, schema.Size);

        table.AddBTree(q.Column, btree);

        // Persist unique flag in schema
        if (q.Unique)
        {
            schema.IsUnique = true;
            table.SaveSchema();
        }

        return new SproutResponse
        {
            Operation = SproutOperation.CreateIndex,
            Affected = 1,
        };
    }
}

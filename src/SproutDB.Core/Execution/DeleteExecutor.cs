using SproutDB.Core.Parsing;
using SproutDB.Core.Storage;

namespace SproutDB.Core.Execution;

internal static class DeleteExecutor
{
    public static SproutResponse Execute(string query, TableHandle table, DeleteQuery q)
    {
        // Validate where tree
        var whereErrors = WhereEngine.ValidateWhereNode(table, q.Where);
        if (whereErrors is not null)
            return ResponseHelper.Errors(query, whereErrors);

        // Prepare compiled filter
        var filter = WhereEngine.PrepareFilter(table, q.Where);
        if (filter is null)
            return new SproutResponse { Operation = SproutOperation.Delete, Affected = 0 };

        // Collect matching rows first, then delete (safe slot-based iteration)
        var toDelete = new List<(ulong Id, long Place)>();
        table.Index.ForEachUsed((id, place) =>
        {
            if (WhereEngine.EvaluateFilter(filter, id, place))
                toDelete.Add((id, place));
        });

        foreach (var (id, place) in toDelete)
        {
            // Remove from B-Trees before clearing data
            foreach (var col in table.Schema.Columns)
            {
                if (table.HasBTree(col.Name))
                {
                    var colHandle = table.GetColumn(col.Name);
                    if (!colHandle.IsNullAtPlace(place))
                    {
                        var val = colHandle.ReadValue(place);
                        if (val is not null)
                        {
                            var encoded = colHandle.EncodeValueToBytes(val.ToString() ?? "");
                            table.GetBTree(col.Name).Remove(encoded, place);
                        }
                    }
                }
            }

            // Delete blob files
            foreach (var col in table.Schema.Columns)
            {
                ColumnTypes.TryParse(col.Type, out var colType);
                if (colType == ColumnType.Blob)
                    table.DeleteBlobFile(col.Name, (long)id);
            }

            // Free slot (marks as deleted, decrements count)
            table.Index.FreeSlot(place);

            // Clear TTL entry
            table.Ttl?.Clear(place);

            // Write null flag for each column
            foreach (var col in table.Schema.Columns)
                table.GetColumn(col.Name).WriteNull(place);
        }

        return new SproutResponse { Operation = SproutOperation.Delete, Affected = toDelete.Count };
    }
}

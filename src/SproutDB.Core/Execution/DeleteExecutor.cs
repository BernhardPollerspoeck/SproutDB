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

        var nextId = table.Index.ReadNextId();
        var deletedCount = 0;

        for (ulong id = 1; id < nextId; id++)
        {
            var place = table.Index.ReadPlace(id);
            if (place < 0)
                continue; // already deleted / free

            if (!WhereEngine.EvaluateFilter(filter, id, place))
                continue;

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

            // Clear index slot (marks row as deleted)
            table.Index.ClearPlace(id);

            // Write null flag for each column
            foreach (var col in table.Schema.Columns)
                table.GetColumn(col.Name).WriteNull(place);

            deletedCount++;

            // Register freed place for reuse
            table.Index.AddFreePlace(place);
        }

        return new SproutResponse { Operation = SproutOperation.Delete, Affected = deletedCount };
    }
}

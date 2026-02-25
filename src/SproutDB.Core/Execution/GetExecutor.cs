using SproutDB.Core.Parsing;
using SproutDB.Core.Storage;

namespace SproutDB.Core.Execution;

internal static class GetExecutor
{
    public static SproutResponse Execute(string query, TableHandle table, GetQuery q)
    {
        // Validate select columns
        if (q.Select is not null)
        {
            foreach (var col in q.Select)
            {
                if (col != "id" && !table.HasColumn(col))
                    return ResponseHelper.Error(query, ErrorCodes.UNKNOWN_COLUMN,
                        $"column '{col}' does not exist");
            }
        }

        var data = ReadRows(table, q.Select).ToList();

        return new SproutResponse
        {
            Operation = SproutOperation.Get,
            Data = data,
            Affected = data.Count,
        };
    }

    private static IEnumerable<Dictionary<string, object?>> ReadRows(
        TableHandle table, List<string>? selectColumns)
    {
        // Determine which columns to project
        var includeId = selectColumns is null || selectColumns.Contains("id");
        var columns = ResolveColumns(table, selectColumns);

        var nextId = table.Index.ReadNextId();

        for (ulong id = 1; id < nextId; id++)
        {
            var place = table.Index.ReadPlace(id);
            if (place < 0)
                continue; // deleted / free slot

            var record = new Dictionary<string, object?>(columns.Count + 1);

            if (includeId)
                record["id"] = id;

            foreach (var (name, handle) in columns)
                record[name] = handle.ReadValue(place);

            yield return record;
        }
    }

    private static List<(string Name, ColumnHandle Handle)> ResolveColumns(
        TableHandle table, List<string>? selectColumns)
    {
        if (selectColumns is null)
        {
            // All columns
            var all = new List<(string, ColumnHandle)>(table.Schema.Columns.Count);
            foreach (var col in table.Schema.Columns)
                all.Add((col.Name, table.GetColumn(col.Name)));
            return all;
        }

        // Only selected columns (excluding "id" which is handled separately)
        var result = new List<(string, ColumnHandle)>(selectColumns.Count);
        foreach (var name in selectColumns)
        {
            if (name == "id") continue;
            result.Add((name, table.GetColumn(name)));
        }
        return result;
    }
}

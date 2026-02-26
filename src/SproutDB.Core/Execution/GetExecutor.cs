using SproutDB.Core.Parsing;
using SproutDB.Core.Storage;

namespace SproutDB.Core.Execution;

internal static class GetExecutor
{
    public static SproutResponse Execute(string query, TableHandle table, GetQuery q)
    {
        // Validate select columns — collect all unknown columns
        if (q.Select is not null)
        {
            List<SproutError>? errors = null;
            foreach (var col in q.Select)
            {
                if (col.Name != "id" && !table.HasColumn(col.Name))
                {
                    errors ??= [];
                    errors.Add(new SproutError { Code = ErrorCodes.UNKNOWN_COLUMN, Message = $"column '{col.Name}' does not exist", Position = col.Position, Length = col.Length });
                }
            }
            if (errors is not null)
                return ResponseHelper.Errors(query, errors);
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
        TableHandle table, List<SelectColumn>? selectColumns)
    {
        // Determine which columns to project
        var includeId = selectColumns is null || selectColumns.Exists(c => c.Name == "id");
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
        TableHandle table, List<SelectColumn>? selectColumns)
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
        foreach (var col in selectColumns)
        {
            if (col.Name == "id") continue;
            result.Add((col.Name, table.GetColumn(col.Name)));
        }
        return result;
    }
}

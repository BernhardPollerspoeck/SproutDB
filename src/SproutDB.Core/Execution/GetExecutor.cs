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

        // Validate where column
        if (q.Where is not null)
        {
            var w = q.Where;
            if (w.Column != "id" && !table.HasColumn(w.Column))
                return ResponseHelper.Error(query, ErrorCodes.UNKNOWN_COLUMN,
                    $"column '{w.Column}' does not exist");
        }

        // Prepare where filter
        var filter = PrepareFilter(table, q.Where);

        var data = ReadRows(table, q.Select, q.ExcludeSelect, filter).ToList();

        return new SproutResponse
        {
            Operation = SproutOperation.Get,
            Data = data,
            Affected = data.Count,
        };
    }

    // ── Filter ────────────────────────────────────────────────

    private static RowFilter? PrepareFilter(TableHandle table, WhereClause? where)
    {
        if (where is null) return null;

        if (where.Column == "id")
        {
            var idValue = ulong.Parse(where.Value);
            return new RowFilter(null, null, where.Operator, idValue);
        }

        var handle = table.GetColumn(where.Column);
        var encoded = handle.EncodeValueToBytes(where.Value);
        return new RowFilter(handle, encoded, where.Operator, 0);
    }

    private static bool MatchesFilter(RowFilter filter, ulong id, long place)
    {
        int cmp;

        if (filter.Handle is null)
        {
            // ID comparison
            cmp = id.CompareTo(filter.IdValue);
        }
        else
        {
            var result = filter.Handle.CompareAtPlace(place, filter.Encoded);
            if (result is null) return false; // null never matches
            cmp = result.Value;
        }

        return filter.Op switch
        {
            CompareOp.Equal => cmp == 0,
            CompareOp.NotEqual => cmp != 0,
            CompareOp.GreaterThan => cmp > 0,
            CompareOp.GreaterThanOrEqual => cmp >= 0,
            CompareOp.LessThan => cmp < 0,
            CompareOp.LessThanOrEqual => cmp <= 0,
            _ => false,
        };
    }

    // ── Read rows ─────────────────────────────────────────────

    private static IEnumerable<Dictionary<string, object?>> ReadRows(
        TableHandle table, List<SelectColumn>? selectColumns, bool excludeSelect, RowFilter? filter)
    {
        // Determine which columns to project
        bool includeId;
        if (excludeSelect)
            includeId = selectColumns is null || !selectColumns.Exists(c => c.Name == "id");
        else
            includeId = selectColumns is null || selectColumns.Exists(c => c.Name == "id");

        var columns = ResolveColumns(table, selectColumns, excludeSelect);

        var nextId = table.Index.ReadNextId();

        for (ulong id = 1; id < nextId; id++)
        {
            var place = table.Index.ReadPlace(id);
            if (place < 0)
                continue; // deleted / free slot

            // Apply where filter
            if (filter is not null && !MatchesFilter(filter, id, place))
                continue;

            var record = new Dictionary<string, object?>(columns.Count + 1);

            if (includeId)
                record["id"] = id;

            foreach (var (name, handle) in columns)
                record[name] = handle.ReadValue(place);

            yield return record;
        }
    }

    private static List<(string Name, ColumnHandle Handle)> ResolveColumns(
        TableHandle table, List<SelectColumn>? selectColumns, bool excludeSelect)
    {
        if (selectColumns is null)
        {
            // All columns
            var all = new List<(string, ColumnHandle)>(table.Schema.Columns.Count);
            foreach (var col in table.Schema.Columns)
                all.Add((col.Name, table.GetColumn(col.Name)));
            return all;
        }

        if (excludeSelect)
        {
            // All columns EXCEPT the named ones
            var excluded = new HashSet<string>(selectColumns.Count);
            foreach (var col in selectColumns)
                excluded.Add(col.Name);

            var result = new List<(string, ColumnHandle)>(table.Schema.Columns.Count);
            foreach (var col in table.Schema.Columns)
            {
                if (!excluded.Contains(col.Name))
                    result.Add((col.Name, table.GetColumn(col.Name)));
            }
            return result;
        }

        {
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

    // ── Filter record ─────────────────────────────────────────

    private sealed record RowFilter(ColumnHandle? Handle, byte[]? Encoded, CompareOp Op, ulong IdValue);
}

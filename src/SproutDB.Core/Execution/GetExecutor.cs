using SproutDB.Core.Parsing;
using SproutDB.Core.Storage;

namespace SproutDB.Core.Execution;

internal static class GetExecutor
{
    public static SproutResponse Execute(string query, TableHandle table, GetQuery q, int defaultPageSize)
    {
        // Validate select columns — collect all unknown columns
        List<SproutError>? validationErrors = null;

        if (q.Select is not null)
        {
            foreach (var col in q.Select)
            {
                if (col.Name != "id" && !table.HasColumn(col.Name))
                {
                    validationErrors ??= [];
                    validationErrors.Add(new SproutError { Code = ErrorCodes.UNKNOWN_COLUMN, Message = $"column '{col.Name}' does not exist", Position = col.Position, Length = col.Length });
                }
            }
        }

        // Validate where tree
        if (q.Where is not null)
        {
            var whereErrors = ValidateWhereNode(table, q.Where);
            validationErrors = MergeErrors(validationErrors, whereErrors);
        }

        // Validate order by columns
        if (q.OrderBy is not null)
        {
            foreach (var col in q.OrderBy)
            {
                if (col.Name != "id" && !table.HasColumn(col.Name))
                {
                    validationErrors ??= [];
                    validationErrors.Add(new SproutError { Code = ErrorCodes.UNKNOWN_COLUMN, Message = $"column '{col.Name}' does not exist", Position = col.Position, Length = col.Length });
                }
            }
        }

        // Validate aggregate column
        if (q.Aggregate.HasValue && q.AggregateColumn is not null)
        {
            if (q.AggregateColumn != "id" && !table.HasColumn(q.AggregateColumn))
            {
                validationErrors ??= [];
                validationErrors.Add(new SproutError { Code = ErrorCodes.UNKNOWN_COLUMN, Message = $"column '{q.AggregateColumn}' does not exist", Position = q.AggregateColumnPosition, Length = q.AggregateColumnLength });
            }
            else if (q.Aggregate is AggregateFunction.Sum or AggregateFunction.Avg
                     && q.AggregateColumn != "id" && !IsNumericColumn(table, q.AggregateColumn))
            {
                validationErrors ??= [];
                validationErrors.Add(new SproutError { Code = ErrorCodes.TYPE_MISMATCH, Message = $"'{AggregateName(q.Aggregate.Value)}' can only be used on numeric columns", Position = q.AggregateColumnPosition, Length = q.AggregateColumnLength });
            }
        }

        if (validationErrors is not null)
            return ResponseHelper.Errors(query, validationErrors);

        // Aggregate path: compute aggregate directly, skip normal row projection
        if (q.Aggregate.HasValue && q.AggregateColumn is not null)
        {
            return ExecuteAggregate(table, q);
        }

        // Prepare where filter
        var filter = PrepareFilter(table, q.Where);

        var data = ReadRows(table, q.Select, q.ExcludeSelect, filter).ToList();

        // Distinct: deduplicate based on projected column values
        if (q.IsDistinct)
        {
            var seen = new HashSet<string>();
            var distinct = new List<Dictionary<string, object?>>(data.Count);
            foreach (var row in data)
            {
                var key = BuildDistinctKey(row);
                if (seen.Add(key))
                    distinct.Add(row);
            }
            data = distinct;
        }

        // Order by
        if (q.OrderBy is not null && q.OrderBy.Count > 0)
        {
            var orderColumns = q.OrderBy;
            data.Sort((a, b) =>
            {
                foreach (var col in orderColumns)
                {
                    a.TryGetValue(col.Name, out var valA);
                    b.TryGetValue(col.Name, out var valB);
                    var cmp = CompareValues(valA, valB);
                    if (col.Descending) cmp = -cmp;
                    if (cmp != 0) return cmp;
                }
                return 0;
            });
        }

        // Count: return count only
        if (q.IsCount)
        {
            return new SproutResponse
            {
                Operation = SproutOperation.Get,
                Data = [],
                Affected = data.Count,
            };
        }

        // Limit (explicit limit disables auto-paging)
        if (q.Limit.HasValue)
        {
            data = data.Take(q.Limit.Value).ToList();
            return new SproutResponse
            {
                Operation = SproutOperation.Get,
                Data = data,
                Affected = data.Count,
            };
        }

        // Manual paging: page N size M
        if (q.Page.HasValue)
        {
            var pageSize = Math.Min(q.Size ?? defaultPageSize, defaultPageSize);
            return ApplyPaging(query, data, q.Page.Value, pageSize);
        }

        // Auto-paging: if result exceeds default page size
        if (data.Count > defaultPageSize)
        {
            return ApplyPaging(query, data, 1, defaultPageSize);
        }

        return new SproutResponse
        {
            Operation = SproutOperation.Get,
            Data = data,
            Affected = data.Count,
        };
    }

    private static SproutResponse ApplyPaging(
        string query, List<Dictionary<string, object?>> data, int page, int pageSize)
    {
        var total = data.Count;
        var skip = (page - 1) * pageSize;
        var pageData = data.Skip(skip).Take(pageSize).ToList();
        var totalPages = (total + pageSize - 1) / pageSize;
        var hasNext = page < totalPages;

        string? nextQuery = null;
        if (hasNext)
        {
            nextQuery = BuildNextPageQuery(query, page + 1, pageSize);
        }

        return new SproutResponse
        {
            Operation = SproutOperation.Get,
            Data = pageData,
            Affected = pageData.Count,
            Paging = new PagingInfo
            {
                Total = total,
                PageSize = pageSize,
                Page = page,
                Next = nextQuery,
            },
        };
    }

    private static string BuildNextPageQuery(string query, int nextPage, int pageSize)
    {
        // If query already has 'page N size M', replace it
        // Otherwise append it
        var pageIdx = query.LastIndexOf(" page ", StringComparison.OrdinalIgnoreCase);
        if (pageIdx >= 0)
        {
            return $"{query[..pageIdx]} page {nextPage} size {pageSize}";
        }

        return $"{query} page {nextPage} size {pageSize}";
    }

    // ── Aggregate ─────────────────────────────────────────────

    private static SproutResponse ExecuteAggregate(TableHandle table, GetQuery q)
    {
        var filter = PrepareFilter(table, q.Where);
        var colName = q.AggregateColumn!; // validated non-null before call
        var fn = q.Aggregate!.Value;
        var alias = q.AggregateAlias ?? AggregateName(fn);

        // Read values for the aggregate column, applying where filter
        var values = ReadAggregateValues(table, colName, filter);

        object? result = fn switch
        {
            AggregateFunction.Sum => ComputeSum(values),
            AggregateFunction.Avg => ComputeAvg(values),
            AggregateFunction.Min => ComputeMinMax(values, min: true),
            AggregateFunction.Max => ComputeMinMax(values, min: false),
            _ => null,
        };

        var row = new Dictionary<string, object?> { [alias] = result };

        return new SproutResponse
        {
            Operation = SproutOperation.Get,
            Data = [row],
            Affected = 1,
        };
    }

    private static List<object> ReadAggregateValues(TableHandle table, string colName, FilterNode? filter)
    {
        var values = new List<object>();
        var nextId = table.Index.ReadNextId();

        // For "id" column, the value is the id itself
        if (colName == "id")
        {
            for (ulong id = 1; id < nextId; id++)
            {
                var place = table.Index.ReadPlace(id);
                if (place < 0) continue;
                if (filter is not null && !EvaluateFilter(filter, id, place)) continue;
                values.Add(id);
            }
            return values;
        }

        var handle = table.GetColumn(colName);
        for (ulong id = 1; id < nextId; id++)
        {
            var place = table.Index.ReadPlace(id);
            if (place < 0) continue;
            if (filter is not null && !EvaluateFilter(filter, id, place)) continue;

            var val = handle.ReadValue(place);
            if (val is not null)
                values.Add(val);
        }
        return values;
    }

    private static double ComputeSum(List<object> values)
    {
        var sum = 0.0;
        foreach (var v in values)
            sum += Convert.ToDouble(v);
        return sum;
    }

    private static double? ComputeAvg(List<object> values)
    {
        if (values.Count == 0) return null;
        return ComputeSum(values) / values.Count;
    }

    private static object? ComputeMinMax(List<object> values, bool min)
    {
        if (values.Count == 0) return null;

        var best = values[0];
        for (var i = 1; i < values.Count; i++)
        {
            if (best is IComparable cmp)
            {
                var comparison = cmp.CompareTo(values[i]);
                if (min ? comparison > 0 : comparison < 0)
                    best = values[i];
            }
        }
        return best;
    }

    private static bool IsNumericColumn(TableHandle table, string colName)
    {
        var handle = table.GetColumn(colName);
        return handle.Type is ColumnType.SByte or ColumnType.UByte
            or ColumnType.SShort or ColumnType.UShort
            or ColumnType.SInt or ColumnType.UInt
            or ColumnType.SLong or ColumnType.ULong
            or ColumnType.Float or ColumnType.Double;
    }

    private static string AggregateName(AggregateFunction fn) => fn switch
    {
        AggregateFunction.Sum => "sum",
        AggregateFunction.Avg => "avg",
        AggregateFunction.Min => "min",
        AggregateFunction.Max => "max",
        _ => fn.ToString().ToLowerInvariant(),
    };

    private static string BuildDistinctKey(Dictionary<string, object?> row)
    {
        // Build a composite key from all column values
        var parts = new string[row.Count];
        var i = 0;
        foreach (var kvp in row)
        {
            parts[i++] = kvp.Value?.ToString() ?? "\0null";
        }
        return string.Join("\x1F", parts);
    }

    private static int CompareValues(object? a, object? b)
    {
        if (a is null && b is null) return 0;
        if (a is null) return -1;
        if (b is null) return 1;

        if (a is IComparable ca)
            return ca.CompareTo(b);

        return string.Compare(a.ToString(), b.ToString(), StringComparison.Ordinal);
    }

    // ── Validation ─────────────────────────────────────────────

    private static List<SproutError>? ValidateWhereNode(TableHandle table, WhereNode node)
    {
        switch (node)
        {
            case CompareNode c:
                return ValidateCompare(table, c);

            case NullCheckNode n:
                if (n.Column != "id" && !table.HasColumn(n.Column))
                    return [new SproutError
                    {
                        Code = ErrorCodes.UNKNOWN_COLUMN,
                        Message = $"column '{n.Column}' does not exist",
                        Position = n.ColumnPosition, Length = n.ColumnLength,
                    }];
                return null;

            case LogicalNode l:
            {
                var leftErrors = ValidateWhereNode(table, l.Left);
                var rightErrors = ValidateWhereNode(table, l.Right);
                return MergeErrors(leftErrors, rightErrors);
            }

            case NotNode not:
                return ValidateWhereNode(table, not.Inner);

            case InNode i:
                if (i.Column != "id" && !table.HasColumn(i.Column))
                    return [new SproutError
                    {
                        Code = ErrorCodes.UNKNOWN_COLUMN,
                        Message = $"column '{i.Column}' does not exist",
                        Position = i.ColumnPosition, Length = i.ColumnLength,
                    }];
                return null;

            default:
                return null;
        }
    }

    private static List<SproutError>? ValidateCompare(TableHandle table, CompareNode c)
    {
        if (c.Column != "id" && !table.HasColumn(c.Column))
            return [new SproutError
            {
                Code = ErrorCodes.UNKNOWN_COLUMN,
                Message = $"column '{c.Column}' does not exist",
                Position = c.ColumnPosition, Length = c.ColumnLength,
            }];

        if (IsStringOp(c.Operator))
        {
            if (c.Column == "id")
                return [new SproutError
                {
                    Code = ErrorCodes.TYPE_MISMATCH,
                    Message = $"'{OpName(c.Operator)}' cannot be used on 'id'",
                    Position = c.ColumnPosition, Length = c.ColumnLength,
                }];

            var colHandle = table.GetColumn(c.Column);
            if (colHandle.Type != ColumnType.String)
                return [new SproutError
                {
                    Code = ErrorCodes.TYPE_MISMATCH,
                    Message = $"'{OpName(c.Operator)}' can only be used on string columns",
                    Position = c.ColumnPosition, Length = c.ColumnLength,
                }];
        }

        return null;
    }

    private static List<SproutError>? MergeErrors(List<SproutError>? a, List<SproutError>? b)
    {
        if (a is null) return b;
        if (b is null) return a;
        a.AddRange(b);
        return a;
    }

    // ── Filter tree ────────────────────────────────────────────

    private abstract class FilterNode;

    private sealed class CompareFilter : FilterNode
    {
        public required ColumnHandle? Handle { get; init; }
        public required byte[]? Encoded { get; init; }
        public required CompareOp Op { get; init; }
        public required ulong IdValue { get; init; }
        public byte[]? Encoded2 { get; init; }
        public ulong IdValue2 { get; init; }
    }

    private sealed class NullFilter : FilterNode
    {
        public required ColumnHandle? Handle { get; init; }
        public required bool IsNot { get; init; }
    }

    private sealed class LogicalFilter : FilterNode
    {
        public required LogicalOp Op { get; init; }
        public required FilterNode Left { get; init; }
        public required FilterNode Right { get; init; }
    }

    private sealed class NotFilter : FilterNode
    {
        public required FilterNode Inner { get; init; }
    }

    private sealed class InFilter : FilterNode
    {
        public required ColumnHandle? Handle { get; init; }
        public required List<byte[]> EncodedValues { get; init; }
        public required List<ulong> IdValues { get; init; }
        public required bool IsNot { get; init; }
    }

    private static FilterNode? PrepareFilter(TableHandle table, WhereNode? node)
    {
        if (node is null) return null;

        switch (node)
        {
            case CompareNode c:
                return PrepareCompareFilter(table, c);

            case NullCheckNode n:
            {
                // id is never null — Handle is null for id column
                ColumnHandle? handle = n.Column == "id" ? null : table.GetColumn(n.Column);
                return new NullFilter { Handle = handle, IsNot = n.IsNot };
            }

            case LogicalNode l:
            {
                var left = PrepareFilter(table, l.Left);
                var right = PrepareFilter(table, l.Right);
                if (left is null || right is null) return null;
                return new LogicalFilter { Op = l.Op, Left = left, Right = right };
            }

            case NotNode not:
            {
                var inner = PrepareFilter(table, not.Inner);
                if (inner is null) return null;
                return new NotFilter { Inner = inner };
            }

            case InNode i:
                return PrepareInFilter(table, i);

            default:
                return null;
        }
    }

    private static InFilter PrepareInFilter(TableHandle table, InNode i)
    {
        if (i.Column == "id")
        {
            var idValues = new List<ulong>(i.Values.Count);
            foreach (var v in i.Values)
                idValues.Add(ulong.Parse(v));
            return new InFilter { Handle = null, EncodedValues = [], IdValues = idValues, IsNot = i.IsNot };
        }

        var handle = table.GetColumn(i.Column);
        var encodedValues = new List<byte[]>(i.Values.Count);
        foreach (var v in i.Values)
            encodedValues.Add(handle.EncodeValueToBytes(v));
        return new InFilter { Handle = handle, EncodedValues = encodedValues, IdValues = [], IsNot = i.IsNot };
    }

    private static CompareFilter PrepareCompareFilter(TableHandle table, CompareNode c)
    {
        if (c.Column == "id")
        {
            var idValue = ulong.Parse(c.Value);
            ulong idValue2 = IsBetweenOp(c.Operator) && c.Value2 is not null
                ? ulong.Parse(c.Value2) : 0;
            return new CompareFilter { Handle = null, Encoded = null, Op = c.Operator, IdValue = idValue, IdValue2 = idValue2 };
        }

        var handle = table.GetColumn(c.Column);

        if (IsStringOp(c.Operator))
        {
            var needle = handle.EncodeStringBytes(c.Value);
            return new CompareFilter { Handle = handle, Encoded = needle, Op = c.Operator, IdValue = 0 };
        }

        var encoded = handle.EncodeValueToBytes(c.Value);
        byte[]? encoded2 = IsBetweenOp(c.Operator) && c.Value2 is not null
            ? handle.EncodeValueToBytes(c.Value2) : null;
        return new CompareFilter { Handle = handle, Encoded = encoded, Op = c.Operator, IdValue = 0, Encoded2 = encoded2 };
    }

    private static bool EvaluateFilter(FilterNode filter, ulong id, long place)
    {
        switch (filter)
        {
            case CompareFilter cf:
                return MatchesCompare(cf, id, place);

            case NullFilter nf:
                return EvaluateNullFilter(nf, id, place);

            case LogicalFilter lf:
                return lf.Op == LogicalOp.And
                    ? EvaluateFilter(lf.Left, id, place) && EvaluateFilter(lf.Right, id, place)
                    : EvaluateFilter(lf.Left, id, place) || EvaluateFilter(lf.Right, id, place);

            case NotFilter notF:
                return !EvaluateFilter(notF.Inner, id, place);

            case InFilter inf:
                return EvaluateInFilter(inf, id, place);

            default:
                return false;
        }
    }

    private static bool EvaluateInFilter(InFilter inf, ulong id, long place)
    {
        bool found;

        if (inf.Handle is null)
        {
            // ID comparison — id is never null
            found = inf.IdValues.Contains(id);
        }
        else
        {
            // Null values never match IN or NOT IN
            if (inf.Handle.IsNullAtPlace(place))
                return false;

            found = false;
            foreach (var encoded in inf.EncodedValues)
            {
                if (inf.Handle.MatchesAtPlace(place, encoded))
                {
                    found = true;
                    break;
                }
            }
        }

        return inf.IsNot ? !found : found;
    }

    private static bool EvaluateNullFilter(NullFilter nf, ulong id, long place)
    {
        if (nf.Handle is null)
        {
            // id is never null
            return nf.IsNot; // IS NOT NULL → true, IS NULL → false
        }

        var isNull = nf.Handle.IsNullAtPlace(place);
        return nf.IsNot ? !isNull : isNull;
    }

    private static bool MatchesCompare(CompareFilter filter, ulong id, long place)
    {
        if (filter.Handle is not null && IsStringOp(filter.Op))
        {
            if (filter.Encoded is null) return false;
            return filter.Handle.StringMatchAtPlace(place, filter.Encoded, filter.Op);
        }

        if (IsBetweenOp(filter.Op))
            return MatchesBetween(filter, id, place);

        int cmp;

        if (filter.Handle is null)
        {
            // ID comparison
            cmp = id.CompareTo(filter.IdValue);
        }
        else
        {
            if (filter.Encoded is null) return false;
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

    private static bool MatchesBetween(CompareFilter filter, ulong id, long place)
    {
        int cmpLow, cmpHigh;

        if (filter.Handle is null)
        {
            cmpLow = id.CompareTo(filter.IdValue);
            cmpHigh = id.CompareTo(filter.IdValue2);
        }
        else
        {
            if (filter.Encoded is null || filter.Encoded2 is null) return false;
            var lo = filter.Handle.CompareAtPlace(place, filter.Encoded);
            if (lo is null) return false;
            var hi = filter.Handle.CompareAtPlace(place, filter.Encoded2);
            if (hi is null) return false;
            cmpLow = lo.Value;
            cmpHigh = hi.Value;
        }

        var inRange = cmpLow >= 0 && cmpHigh <= 0;
        return filter.Op == CompareOp.Between ? inRange : !inRange;
    }

    // ── Read rows ─────────────────────────────────────────────

    private static IEnumerable<Dictionary<string, object?>> ReadRows(
        TableHandle table, List<SelectColumn>? selectColumns, bool excludeSelect, FilterNode? filter)
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
            if (filter is not null && !EvaluateFilter(filter, id, place))
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

    // ── Helpers ─────────────────────────────────────────────────

    private static bool IsStringOp(CompareOp op) =>
        op is CompareOp.Contains or CompareOp.StartsWith or CompareOp.EndsWith;

    private static string OpName(CompareOp op) => op switch
    {
        CompareOp.Contains => "contains",
        CompareOp.StartsWith => "starts",
        CompareOp.EndsWith => "ends",
        _ => op.ToString(),
    };

    private static bool IsBetweenOp(CompareOp op) =>
        op is CompareOp.Between or CompareOp.NotBetween;
}

using SproutDB.Core.Parsing;
using SproutDB.Core.Storage;

namespace SproutDB.Core.Execution;

internal static class WhereEngine
{
    // ── Filter tree ────────────────────────────────────────────

    internal abstract class FilterNode;

    internal sealed class CompareFilter : FilterNode
    {
        public required ColumnHandle? Handle { get; init; }
        public required byte[]? Encoded { get; init; }
        public required CompareOp Op { get; init; }
        public required ulong IdValue { get; init; }
        public byte[]? Encoded2 { get; init; }
        public ulong IdValue2 { get; init; }
    }

    internal sealed class NullFilter : FilterNode
    {
        public required ColumnHandle? Handle { get; init; }
        public required bool IsNot { get; init; }
    }

    internal sealed class LogicalFilter : FilterNode
    {
        public required LogicalOp Op { get; init; }
        public required FilterNode Left { get; init; }
        public required FilterNode Right { get; init; }
    }

    internal sealed class NotFilter : FilterNode
    {
        public required FilterNode Inner { get; init; }
    }

    internal sealed class InFilter : FilterNode
    {
        public required ColumnHandle? Handle { get; init; }
        public required List<byte[]> EncodedValues { get; init; }
        public required List<ulong> IdValues { get; init; }
        public required bool IsNot { get; init; }
    }

    // ── Prepare ────────────────────────────────────────────────

    internal static FilterNode? PrepareFilter(TableHandle table, WhereNode? node)
    {
        if (node is null) return null;

        switch (node)
        {
            case CompareNode c:
                return PrepareCompareFilter(table, c);

            case NullCheckNode n:
            {
                ColumnHandle? handle = n.Column == "_id" ? null : table.GetColumn(n.Column);
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
        if (i.Column == "_id")
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
        if (c.Column == "_id")
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

    // ── Evaluate ───────────────────────────────────────────────

    internal static bool EvaluateFilter(FilterNode filter, ulong id, long place)
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
            found = inf.IdValues.Contains(id);
        }
        else
        {
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
            return nf.IsNot; // id is never null
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
            cmp = id.CompareTo(filter.IdValue);
        }
        else
        {
            if (filter.Encoded is null) return false;
            var result = filter.Handle.CompareAtPlace(place, filter.Encoded);
            if (result is null) return false;
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

    // ── Validation ─────────────────────────────────────────────

    internal static List<SproutError>? ValidateWhereNode(TableHandle table, WhereNode node)
    {
        switch (node)
        {
            case CompareNode c:
                return ValidateCompare(table, c);

            case NullCheckNode n:
                if (n.Column != "_id" && !table.HasColumn(n.Column))
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
                if (i.Column != "_id" && !table.HasColumn(i.Column))
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
        if (c.Column != "_id" && !table.HasColumn(c.Column))
            return [new SproutError
            {
                Code = ErrorCodes.UNKNOWN_COLUMN,
                Message = $"column '{c.Column}' does not exist",
                Position = c.ColumnPosition, Length = c.ColumnLength,
            }];

        if (IsStringOp(c.Operator))
        {
            if (c.Column == "_id")
                return [new SproutError
                {
                    Code = ErrorCodes.TYPE_MISMATCH,
                    Message = $"'{OpName(c.Operator)}' cannot be used on '_id'",
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

    internal static List<SproutError>? MergeErrors(List<SproutError>? a, List<SproutError>? b)
    {
        if (a is null) return b;
        if (b is null) return a;
        a.AddRange(b);
        return a;
    }

    // ── Column extraction ──────────────────────────────────────

    /// <summary>
    /// Extracts all column names referenced in a WHERE tree.
    /// </summary>
    internal static HashSet<string> ExtractWhereColumns(WhereNode? node)
    {
        var columns = new HashSet<string>();
        if (node is not null)
            CollectColumns(node, columns);
        return columns;
    }

    private static void CollectColumns(WhereNode node, HashSet<string> columns)
    {
        switch (node)
        {
            case CompareNode c:
                if (c.Column != "_id")
                    columns.Add(c.Column);
                break;

            case NullCheckNode n:
                if (n.Column != "_id")
                    columns.Add(n.Column);
                break;

            case LogicalNode l:
                CollectColumns(l.Left, columns);
                CollectColumns(l.Right, columns);
                break;

            case NotNode not:
                CollectColumns(not.Inner, columns);
                break;

            case InNode i:
                if (i.Column != "_id")
                    columns.Add(i.Column);
                break;
        }
    }

    // ── Helpers ────────────────────────────────────────────────

    internal static bool IsStringOp(CompareOp op) =>
        op is CompareOp.Contains or CompareOp.StartsWith or CompareOp.EndsWith;

    internal static string OpName(CompareOp op) => op switch
    {
        CompareOp.Contains => "contains",
        CompareOp.StartsWith => "starts",
        CompareOp.EndsWith => "ends",
        _ => op.ToString(),
    };

    internal static bool IsBetweenOp(CompareOp op) =>
        op is CompareOp.Between or CompareOp.NotBetween;
}

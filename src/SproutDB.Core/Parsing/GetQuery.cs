namespace SproutDB.Core.Parsing;

internal sealed class GetQuery : IQuery
{
    public SproutOperation Operation => SproutOperation.Get;
    public required string Table { get; init; }

    /// <summary>
    /// Column names for select projection. Null means all columns.
    /// When <see cref="ExcludeSelect"/> is true, these are columns to exclude instead.
    /// </summary>
    public List<SelectColumn>? Select { get; init; }

    /// <summary>
    /// When true, <see cref="Select"/> lists columns to exclude (all others returned).
    /// </summary>
    public bool ExcludeSelect { get; init; }

    /// <summary>
    /// When true, only distinct rows (based on projected columns) are returned.
    /// Only valid when <see cref="Select"/> is set.
    /// </summary>
    public bool IsDistinct { get; init; }

    /// <summary>
    /// Optional WHERE condition. Null means no filtering.
    /// </summary>
    public WhereNode? Where { get; init; }

    /// <summary>
    /// Optional ORDER BY columns. Null means no sorting.
    /// </summary>
    public List<OrderByColumn>? OrderBy { get; init; }

    /// <summary>
    /// Optional row limit. Null means no limit.
    /// </summary>
    public int? Limit { get; init; }

    /// <summary>
    /// When true, returns only the count (Affected) with empty Data.
    /// </summary>
    public bool IsCount { get; init; }

    /// <summary>
    /// Manual page number (1-based). Null means no manual paging.
    /// </summary>
    public int? Page { get; init; }

    /// <summary>
    /// Manual page size override. Null means use default page size.
    /// </summary>
    public int? Size { get; init; }

    /// <summary>
    /// Aggregate function (sum, avg, min, max). Null means no aggregation.
    /// </summary>
    public AggregateFunction? Aggregate { get; init; }

    /// <summary>
    /// Column name for the aggregate function.
    /// </summary>
    public string? AggregateColumn { get; init; }

    /// <summary>Position of the aggregate column token in the query string.</summary>
    public int AggregateColumnPosition { get; init; }

    /// <summary>Length of the aggregate column token in the query string.</summary>
    public int AggregateColumnLength { get; init; }

    /// <summary>
    /// Optional alias for the aggregate result (from 'as' keyword). Null means use function name.
    /// </summary>
    public string? AggregateAlias { get; init; }
}

internal enum AggregateFunction
{
    Sum,
    Avg,
    Min,
    Max,
}

internal readonly struct SelectColumn(string name, int position, int length)
{
    public string Name { get; } = name;
    public int Position { get; } = position;
    public int Length { get; } = length;
}

internal readonly struct OrderByColumn(string name, int position, int length, bool descending)
{
    public string Name { get; } = name;
    public int Position { get; } = position;
    public int Length { get; } = length;
    public bool Descending { get; } = descending;
}

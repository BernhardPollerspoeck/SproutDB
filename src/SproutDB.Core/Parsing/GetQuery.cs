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
    /// Computed select columns (arithmetic expressions with alias). Null means none.
    /// </summary>
    public List<ComputedColumn>? ComputedSelect { get; init; }

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

    /// <summary>
    /// Optional GROUP BY columns. Null means no grouping.
    /// </summary>
    public List<SelectColumn>? GroupBy { get; init; }

    /// <summary>
    /// Optional follow (join) clauses. Null means no joins.
    /// </summary>
    public List<FollowClause>? Follow { get; init; }
}

internal enum AggregateFunction
{
    Sum,
    Avg,
    Min,
    Max,
}

internal readonly struct SelectColumn(string name, int position, int length, string? alias = null)
{
    public string Name { get; } = name;
    public int Position { get; } = position;
    public int Length { get; } = length;
    public string? Alias { get; } = alias;

    /// <summary>Returns the alias if set, otherwise the original column name.</summary>
    public string OutputName => Alias ?? Name;
}

internal readonly struct OrderByColumn(string name, int position, int length, bool descending)
{
    public string Name { get; } = name;
    public int Position { get; } = position;
    public int Length { get; } = length;
    public bool Descending { get; } = descending;
}

internal enum ArithmeticOp { Add, Subtract, Multiply, Divide }

internal sealed class ComputedColumn
{
    /// <summary>Left operand column name.</summary>
    public required string LeftColumn { get; init; }
    public int LeftPosition { get; init; }
    public int LeftLength { get; init; }

    /// <summary>Arithmetic operator.</summary>
    public required ArithmeticOp Operator { get; init; }

    /// <summary>Right operand column name (null if right is a literal).</summary>
    public string? RightColumn { get; init; }
    public int RightPosition { get; init; }
    public int RightLength { get; init; }

    /// <summary>Right operand literal value (null if right is a column).</summary>
    public double? RightLiteral { get; init; }

    /// <summary>Alias for the computed result.</summary>
    public required string Alias { get; init; }
}

internal enum JoinType : byte
{
    Inner,  // ->
    Left,   // ->?
    Right,  // ?->
    Outer,  // ?->?
}

internal sealed class FollowClause
{
    /// <summary>Source table name (e.g. "users").</summary>
    public required string SourceTable { get; init; }

    /// <summary>Source column name (e.g. "_id").</summary>
    public required string SourceColumn { get; init; }

    public int SourceColumnPosition { get; init; }
    public int SourceColumnLength { get; init; }

    /// <summary>Target table name (e.g. "orders").</summary>
    public required string TargetTable { get; init; }

    public int TargetTablePosition { get; init; }
    public int TargetTableLength { get; init; }

    /// <summary>Target column name (e.g. "user_id").</summary>
    public required string TargetColumn { get; init; }

    public int TargetColumnPosition { get; init; }
    public int TargetColumnLength { get; init; }

    /// <summary>Join type: Inner (->), Left (->?), Right (?->), Outer (?->?).</summary>
    public JoinType JoinType { get; init; }

    /// <summary>Alias for the nested result array (e.g. "orders").</summary>
    public required string Alias { get; init; }

    /// <summary>Optional SELECT projection for the target table columns.</summary>
    public List<SelectColumn>? Select { get; init; }

    /// <summary>Optional WHERE filter for the target table rows.</summary>
    public WhereNode? Where { get; init; }
}

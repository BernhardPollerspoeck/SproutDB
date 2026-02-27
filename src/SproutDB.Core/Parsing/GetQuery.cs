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
    /// Optional WHERE condition. Null means no filtering.
    /// </summary>
    public WhereClause? Where { get; init; }
}

internal readonly struct SelectColumn(string name, int position, int length)
{
    public string Name { get; } = name;
    public int Position { get; } = position;
    public int Length { get; } = length;
}

namespace SproutDB.Core.Parsing;

internal sealed class GetQuery : IQuery
{
    public SproutOperation Operation => SproutOperation.Get;
    public required string Table { get; init; }

    /// <summary>
    /// Column names for select projection. Null means all columns.
    /// </summary>
    public List<SelectColumn>? Select { get; init; }
}

internal readonly struct SelectColumn(string name, int position, int length)
{
    public string Name { get; } = name;
    public int Position { get; } = position;
    public int Length { get; } = length;
}

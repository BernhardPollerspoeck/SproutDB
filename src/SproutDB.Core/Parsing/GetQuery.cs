namespace SproutDB.Core.Parsing;

internal sealed class GetQuery : IQuery
{
    public SproutOperation Operation => SproutOperation.Get;
    public required string Table { get; init; }

    /// <summary>
    /// Column names for select projection. Null means all columns.
    /// </summary>
    public List<string>? Select { get; init; }
}

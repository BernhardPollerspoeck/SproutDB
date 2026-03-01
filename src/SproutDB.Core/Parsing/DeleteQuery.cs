namespace SproutDB.Core.Parsing;

internal sealed class DeleteQuery : IQuery
{
    public SproutOperation Operation => SproutOperation.Delete;
    public required string Table { get; init; }
    public required WhereNode Where { get; init; }
}

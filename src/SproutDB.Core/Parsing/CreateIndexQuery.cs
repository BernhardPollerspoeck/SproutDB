namespace SproutDB.Core.Parsing;

internal sealed class CreateIndexQuery : IQuery
{
    public SproutOperation Operation => SproutOperation.CreateIndex;
    public required string Table { get; init; }
    public required string Column { get; init; }
    public bool Unique { get; init; }
}

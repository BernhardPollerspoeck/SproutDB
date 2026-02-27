namespace SproutDB.Core.Parsing;

internal sealed class PurgeTableQuery : IQuery
{
    public SproutOperation Operation => SproutOperation.PurgeTable;
    public required string Table { get; init; }
}

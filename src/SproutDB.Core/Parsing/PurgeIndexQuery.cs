namespace SproutDB.Core.Parsing;

internal sealed class PurgeIndexQuery : IQuery
{
    public SproutOperation Operation => SproutOperation.PurgeIndex;
    public required string Table { get; init; }
    public required string Column { get; init; }
}

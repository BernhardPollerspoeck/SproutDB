namespace SproutDB.Core.Parsing;

internal sealed class PurgeColumnQuery : IQuery
{
    public SproutOperation Operation => SproutOperation.PurgeColumn;
    public required string Table { get; init; }
    public required string Column { get; init; }
}

namespace SproutDB.Core.Parsing;

internal sealed class AlterColumnQuery : IQuery
{
    public SproutOperation Operation => SproutOperation.AlterColumn;
    public required string Table { get; init; }
    public required string Column { get; init; }
    public required int NewSize { get; init; }
}

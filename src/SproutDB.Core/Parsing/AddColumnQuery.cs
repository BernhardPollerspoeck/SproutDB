namespace SproutDB.Core.Parsing;

internal sealed class AddColumnQuery : IQuery
{
    public SproutOperation Operation => SproutOperation.AddColumn;
    public required string Table { get; init; }
    public required ColumnDefinition Column { get; init; }
}

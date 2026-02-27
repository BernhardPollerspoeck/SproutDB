namespace SproutDB.Core.Parsing;

internal sealed class RenameColumnQuery : IQuery
{
    public SproutOperation Operation => SproutOperation.RenameColumn;
    public required string Table { get; init; }
    public required string OldColumn { get; init; }
    public required string NewColumn { get; init; }
}

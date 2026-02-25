namespace SproutDB.Core.Parsing;

internal sealed class CreateTableQuery : IQuery
{
    public SproutOperation Operation => SproutOperation.CreateTable;
    public required string Table { get; init; }
    public required List<ColumnDefinition> Columns { get; init; }
}

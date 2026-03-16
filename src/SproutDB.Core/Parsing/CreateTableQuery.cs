namespace SproutDB.Core.Parsing;

internal sealed class CreateTableQuery : IQuery
{
    public SproutOperation Operation => SproutOperation.CreateTable;
    public required string Table { get; init; }
    public required List<ColumnDefinition> Columns { get; init; }
    public long TtlSeconds { get; init; } // 0 = no TTL
    public int ChunkSize { get; init; } // 0 = use database/engine default
}

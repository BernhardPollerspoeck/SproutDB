namespace SproutDB.Core.Parsing;

internal sealed class ShrinkTableQuery : IQuery
{
    public SproutOperation Operation => SproutOperation.ShrinkTable;
    public required string Table { get; init; }
    public int ChunkSize { get; init; } // 0 = keep existing
}

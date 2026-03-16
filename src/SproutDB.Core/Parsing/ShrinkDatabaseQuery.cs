namespace SproutDB.Core.Parsing;

internal sealed class ShrinkDatabaseQuery : IQuery
{
    public SproutOperation Operation => SproutOperation.ShrinkDatabase;
    public int ChunkSize { get; init; } // 0 = keep existing
}

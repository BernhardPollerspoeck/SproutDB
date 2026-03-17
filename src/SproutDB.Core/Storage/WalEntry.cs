namespace SproutDB.Core.Storage;

internal readonly struct WalEntry
{
    public required long Sequence { get; init; }
    public required ulong ResolvedId { get; init; }
    public required string Query { get; init; }
    public long GroupId { get; init; }
}

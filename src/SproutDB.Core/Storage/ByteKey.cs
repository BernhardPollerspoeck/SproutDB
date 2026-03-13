namespace SproutDB.Core.Storage;

/// <summary>
/// Wrapper for byte[] that implements value equality for use as dictionary/set key.
/// </summary>
internal readonly struct ByteKey : IEquatable<ByteKey>
{
    public readonly byte[] Bytes;

    public ByteKey(byte[] bytes) => Bytes = bytes;

    public bool Equals(ByteKey other) => Bytes.AsSpan().SequenceEqual(other.Bytes);
    public override bool Equals(object? obj) => obj is ByteKey other && Equals(other);

    public override int GetHashCode()
    {
        var hash = new HashCode();
        foreach (var b in Bytes)
            hash.Add(b);
        return hash.ToHashCode();
    }
}

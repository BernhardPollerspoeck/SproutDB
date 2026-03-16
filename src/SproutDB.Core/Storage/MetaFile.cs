namespace SproutDB.Core.Storage;

/// <summary>
/// Binary meta file reader/writer for _meta.bin.
///
/// Layout:
///   [8 bytes] created_ticks (long, UTC)
///   [4 bytes] chunk_size (int, 0 = use engine default)
/// </summary>
internal static class MetaFile
{
    public static void Write(string path, long createdTicks, int chunkSize = 0)
    {
        using var fs = new FileStream(path, FileMode.Create, FileAccess.Write, FileShare.None);
        using var bw = new BinaryWriter(fs);
        bw.Write(createdTicks);
        bw.Write(chunkSize);
    }

    public static (long CreatedTicks, int ChunkSize) Read(string path)
    {
        using var fs = new FileStream(path, FileMode.Open, FileAccess.Read, FileShare.Read);
        using var br = new BinaryReader(fs);
        var createdTicks = br.ReadInt64();
        var chunkSize = fs.Length > 8 ? br.ReadInt32() : 0;
        return (createdTicks, chunkSize);
    }
}

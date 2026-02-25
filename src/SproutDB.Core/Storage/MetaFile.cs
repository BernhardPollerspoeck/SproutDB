namespace SproutDB.Core.Storage;

/// <summary>
/// Binary meta file reader/writer for _meta.bin.
///
/// Layout:
///   [8 bytes] created_ticks (long, UTC)
/// </summary>
internal static class MetaFile
{
    public static void Write(string path, long createdTicks)
    {
        using var fs = new FileStream(path, FileMode.Create, FileAccess.Write, FileShare.None);
        using var bw = new BinaryWriter(fs);
        bw.Write(createdTicks);
    }

    public static long ReadCreatedTicks(string path)
    {
        using var fs = new FileStream(path, FileMode.Open, FileAccess.Read, FileShare.Read);
        using var br = new BinaryReader(fs);
        return br.ReadInt64();
    }
}

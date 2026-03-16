using SproutDB.Core.Storage;

namespace SproutDB.Core.Execution;

internal static class ShrinkTableExecutor
{
    private const int INDEX_HEADER_SIZE = 20;
    private const int INDEX_SLOT_SIZE = 8;
    private const int TTL_ENTRY_SIZE = 16;

    internal sealed class SlotInfo
    {
        public required List<(long OldPlace, ulong Id)> OccupiedSlots { get; init; }
        public required long NextId { get; init; }
        public required long BeforeBytes { get; init; }
    }

    /// <summary>
    /// Collects slot info from a live table handle (before eviction).
    /// </summary>
    public static SlotInfo CollectSlotInfo(TableHandle table)
    {
        var occupiedSlots = new List<(long OldPlace, ulong Id)>((int)table.Index.ActiveRowCount);
        table.Index.ForEachUsed((id, place) => occupiedSlots.Add((place, id)));
        occupiedSlots.Sort((a, b) => a.OldPlace.CompareTo(b.OldPlace));

        return new SlotInfo
        {
            OccupiedSlots = occupiedSlots,
            NextId = (long)table.Index.ReadNextId(),
            BeforeBytes = table.GetStorageSizeBytes(),
        };
    }

    /// <summary>
    /// Shrinks a table on disk. The table handle must already be flushed and evicted.
    /// Uses pre-collected slot info from CollectSlotInfo.
    /// </summary>
    public static SproutResponse Execute(string query, string tablePath, string tableName, int chunkSize, SlotInfo info)
    {
        var rowCount = info.OccupiedSlots.Count;

        // Compute target slots: max(chunkSize, ceil(rows / chunkSize) * chunkSize)
        int targetSlots;
        if (rowCount == 0)
            targetSlots = chunkSize;
        else
            targetSlots = Math.Max(chunkSize, ((rowCount + chunkSize - 1) / chunkSize) * chunkSize);

        // Rebuild _index
        RebuildIndex(tablePath, info.OccupiedSlots, targetSlots, info.NextId);

        // Rebuild each column file
        var schema = SchemaFile.Read(Path.Combine(tablePath, "_schema.bin"));
        foreach (var col in schema.Columns)
        {
            var colPath = Path.Combine(tablePath, $"{col.Name}.col");
            if (File.Exists(colPath))
                RebuildColumnFile(colPath, col.EntrySize, info.OccupiedSlots, targetSlots);
        }

        // Rebuild _ttl if exists
        var ttlPath = Path.Combine(tablePath, "_ttl");
        if (File.Exists(ttlPath))
            RebuildTtlFile(ttlPath, info.OccupiedSlots, targetSlots);

        // Update schema with new ChunkSize
        if (chunkSize != schema.ChunkSize)
        {
            schema.ChunkSize = chunkSize;
            SchemaFile.Write(Path.Combine(tablePath, "_schema.bin"), schema);
        }

        var afterBytes = GetDirectorySize(tablePath);

        return new SproutResponse
        {
            Operation = SproutOperation.ShrinkTable,
            Schema = new SchemaInfo { Table = tableName },
            Data =
            [
                new Dictionary<string, object?>
                {
                    ["before_bytes"] = info.BeforeBytes,
                    ["after_bytes"] = afterBytes,
                    ["rows"] = rowCount,
                    ["chunk_size"] = chunkSize,
                }
            ],
        };
    }

    private static void RebuildIndex(string tablePath, List<(long OldPlace, ulong Id)> occupiedSlots, int targetSlots, long nextId)
    {
        var indexPath = Path.Combine(tablePath, "_index");
        var tmpPath = indexPath + ".tmp";

        var fileSize = INDEX_HEADER_SIZE + (long)targetSlots * INDEX_SLOT_SIZE;
        using (var fs = File.Create(tmpPath))
        {
            fs.SetLength(fileSize);

            // Write header
            Span<byte> header = stackalloc byte[INDEX_HEADER_SIZE];
            BitConverter.TryWriteBytes(header[0..4], occupiedSlots.Count);   // Count
            BitConverter.TryWriteBytes(header[4..12], nextId);                // NextId preserved
            BitConverter.TryWriteBytes(header[12..16], 0);                    // LowestUsed = 0
            BitConverter.TryWriteBytes(header[16..20], targetSlots);          // TotalSlots
            fs.Seek(0, SeekOrigin.Begin);
            fs.Write(header);

            // Write slots compactly
            Span<byte> slotBuf = stackalloc byte[INDEX_SLOT_SIZE];
            for (int i = 0; i < occupiedSlots.Count; i++)
            {
                var offset = INDEX_HEADER_SIZE + (long)i * INDEX_SLOT_SIZE;
                fs.Seek(offset, SeekOrigin.Begin);
                BitConverter.TryWriteBytes(slotBuf, (long)occupiedSlots[i].Id);
                fs.Write(slotBuf);
            }
        }

        File.Delete(indexPath);
        File.Move(tmpPath, indexPath);
    }

    private static void RebuildColumnFile(string colPath, int entrySize, List<(long OldPlace, ulong Id)> occupiedSlots, int targetSlots)
    {
        var tmpPath = colPath + ".tmp";

        using (var oldFs = new FileStream(colPath, FileMode.Open, FileAccess.Read, FileShare.Read))
        using (var newFs = File.Create(tmpPath))
        {
            newFs.SetLength((long)targetSlots * entrySize);

            var buf = new byte[entrySize];
            for (int i = 0; i < occupiedSlots.Count; i++)
            {
                var oldOffset = occupiedSlots[i].OldPlace * entrySize;
                oldFs.Seek(oldOffset, SeekOrigin.Begin);
                oldFs.ReadExactly(buf);

                var newOffset = (long)i * entrySize;
                newFs.Seek(newOffset, SeekOrigin.Begin);
                newFs.Write(buf);
            }
        }

        File.Delete(colPath);
        File.Move(tmpPath, colPath);
    }

    private static void RebuildTtlFile(string ttlPath, List<(long OldPlace, ulong Id)> occupiedSlots, int targetSlots)
    {
        var tmpPath = ttlPath + ".tmp";

        using (var oldFs = new FileStream(ttlPath, FileMode.Open, FileAccess.Read, FileShare.Read))
        using (var newFs = File.Create(tmpPath))
        {
            newFs.SetLength((long)targetSlots * TTL_ENTRY_SIZE);

            var buf = new byte[TTL_ENTRY_SIZE];
            for (int i = 0; i < occupiedSlots.Count; i++)
            {
                var oldOffset = occupiedSlots[i].OldPlace * TTL_ENTRY_SIZE;
                if (oldOffset + TTL_ENTRY_SIZE > oldFs.Length)
                    continue;

                oldFs.Seek(oldOffset, SeekOrigin.Begin);
                oldFs.ReadExactly(buf);

                var newOffset = (long)i * TTL_ENTRY_SIZE;
                newFs.Seek(newOffset, SeekOrigin.Begin);
                newFs.Write(buf);
            }
        }

        File.Delete(ttlPath);
        File.Move(tmpPath, ttlPath);
    }

    private static long GetDirectorySize(string path)
    {
        long total = 0;
        foreach (var file in Directory.EnumerateFiles(path))
            total += new FileInfo(file).Length;
        return total;
    }
}

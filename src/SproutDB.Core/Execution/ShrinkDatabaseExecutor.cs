using SproutDB.Core.Storage;

namespace SproutDB.Core.Execution;

internal static class ShrinkDatabaseExecutor
{
    public static SproutResponse Execute(
        string query, string dbName, string dbPath, int chunkSize,
        int engineChunkSize,
        Func<string, TableHandle> getTable,
        Action<string> flushAndEvictTable)
    {
        // Update DB meta with new ChunkSize if specified
        if (chunkSize > 0)
        {
            var metaPath = Path.Combine(dbPath, "_meta.bin");
            var (createdTicks, _) = MetaFile.Read(metaPath);
            MetaFile.Write(metaPath, createdTicks, chunkSize);
        }

        // Determine effective DB chunk_size for tables without their own
        int effectiveDbChunkSize;
        if (chunkSize > 0)
            effectiveDbChunkSize = chunkSize;
        else
        {
            var metaPath = Path.Combine(dbPath, "_meta.bin");
            var (_, dbCs) = MetaFile.Read(metaPath);
            effectiveDbChunkSize = dbCs > 0 ? dbCs : engineChunkSize;
        }

        var data = new List<Dictionary<string, object?>>();

        foreach (var tableDir in Directory.GetDirectories(dbPath))
        {
            var schemaPath = Path.Combine(tableDir, "_schema.bin");
            if (!File.Exists(schemaPath))
                continue;

            var tableName = Path.GetFileName(tableDir);
            var schema = SchemaFile.Read(schemaPath);

            // Skip tables with their own chunk_size
            if (schema.ChunkSize > 0)
            {
                data.Add(new Dictionary<string, object?>
                {
                    ["table"] = tableName,
                    ["skipped"] = true,
                    ["reason"] = $"has table-level chunk_size {schema.ChunkSize}",
                });
                continue;
            }

            // Collect slot info while handle is open
            var table = getTable(tableName);
            var slotInfo = ShrinkTableExecutor.CollectSlotInfo(table);

            // Flush and evict (releases MMFs)
            flushAndEvictTable(tableDir);

            // Shrink on disk
            var result = ShrinkTableExecutor.Execute(query, tableDir, tableName, effectiveDbChunkSize, slotInfo);

            if (result.Data is { Count: > 0 })
            {
                var row = result.Data[0];
                row["table"] = tableName;
                data.Add(row);
            }
        }

        return new SproutResponse
        {
            Operation = SproutOperation.ShrinkDatabase,
            Schema = new SchemaInfo { Database = dbName },
            Data = data,
        };
    }
}

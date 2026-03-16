using SproutDB.Core.Storage;

namespace SproutDB.Core.Execution;

internal static class DescribeExecutor
{
    /// <summary>
    /// DESCRIBE TABLE — returns all columns with type info.
    /// </summary>
    public static SproutResponse ExecuteTable(string query, TableHandle table, string tableName, int effectiveChunkSize, Func<string, bool>? isAutoIndex = null)
    {
        var columns = ResponseHelper.BuildColumnInfoList(table.Schema, table.HasBTree, isAutoIndex);

        return new SproutResponse
        {
            Operation = SproutOperation.Describe,
            Schema = new SchemaInfo
            {
                Table = tableName,
                Columns = columns,
                TtlSeconds = table.Schema.TtlSeconds,
                ChunkSize = table.Schema.ChunkSize,
                EffectiveChunkSize = effectiveChunkSize,
            },
        };
    }

    /// <summary>
    /// DESCRIBE ALL — returns all table names in the database.
    /// </summary>
    public static SproutResponse ExecuteAll(string query, string dbPath)
    {
        var tables = new List<string>();

        if (Directory.Exists(dbPath))
        {
            foreach (var dir in Directory.GetDirectories(dbPath))
            {
                var schemaPath = Path.Combine(dir, "_schema.bin");
                if (File.Exists(schemaPath))
                    tables.Add(Path.GetFileName(dir));
            }

            tables.Sort(StringComparer.Ordinal);
        }

        // Read DB-level ChunkSize
        int dbChunkSize = 0;
        var metaPath = Path.Combine(dbPath, "_meta.bin");
        if (File.Exists(metaPath))
        {
            var (_, cs) = MetaFile.Read(metaPath);
            dbChunkSize = cs;
        }

        return new SproutResponse
        {
            Operation = SproutOperation.Describe,
            Schema = new SchemaInfo
            {
                Tables = tables,
                ChunkSize = dbChunkSize,
            },
        };
    }
}

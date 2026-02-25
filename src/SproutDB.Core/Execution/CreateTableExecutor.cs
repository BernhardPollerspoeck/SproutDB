using SproutDB.Core.Parsing;
using SproutDB.Core.Storage;

namespace SproutDB.Core.Execution;

internal static class CreateTableExecutor
{
    public static SproutResponse Execute(string query, string database, string dbPath, CreateTableQuery q)
    {
        if (!Directory.Exists(dbPath))
            return ResponseHelper.Error(query, ErrorCodes.UNKNOWN_DATABASE,
                $"database '{database}' does not exist");

        var tablePath = Path.Combine(dbPath, q.Table);

        if (Directory.Exists(tablePath))
            return ResponseHelper.Error(query, ErrorCodes.TABLE_EXISTS,
                $"table '{q.Table}' already exists");

        Directory.CreateDirectory(tablePath);

        // Build schema
        var schemaColumns = new List<ColumnSchemaEntry>(q.Columns.Count);
        foreach (var col in q.Columns)
        {
            schemaColumns.Add(new ColumnSchemaEntry
            {
                Name = col.Name,
                Type = ColumnTypes.GetName(col.Type),
                Size = col.Size,
                EntrySize = col.EntrySize,
                Nullable = col.IsNullable,
                Default = col.Default,
                Strict = col.Strict,
            });
        }

        var schema = new TableSchema
        {
            CreatedTicks = DateTime.UtcNow.Ticks,
            Columns = schemaColumns,
        };

        SchemaFile.Write(Path.Combine(tablePath, "_schema.bin"), schema);

        // Create _index file (pre-allocated, all zeros = free)
        // Slot 0 stores next_id as ulong
        var indexPath = Path.Combine(tablePath, "_index");
        CreatePreAllocatedFile(
            indexPath,
            (long)(StorageConstants.CHUNK_SIZE + 1) * StorageConstants.INDEX_ENTRY_SIZE);

        // Write initial next_id = 1 into index header
        using (var fs = new FileStream(indexPath, FileMode.Open, FileAccess.Write, FileShare.None))
        {
            Span<byte> buf = stackalloc byte[8];
            System.Buffers.Binary.BinaryPrimitives.WriteUInt64LittleEndian(buf, 1);
            fs.Write(buf);
        }

        // Create .col files for each column
        foreach (var col in q.Columns)
        {
            CreatePreAllocatedFile(
                Path.Combine(tablePath, $"{col.Name}.col"),
                (long)StorageConstants.CHUNK_SIZE * col.EntrySize);
        }

        return new SproutResponse
        {
            Operation = SproutOperation.CreateTable,
            Schema = new SchemaInfo
            {
                Table = q.Table,
                Columns = ResponseHelper.BuildColumnInfoList(q.Columns),
            },
        };
    }

    private static void CreatePreAllocatedFile(string path, long size)
    {
        using var fs = File.Create(path);
        fs.SetLength(size);
    }
}

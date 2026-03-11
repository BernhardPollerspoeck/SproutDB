using SproutDB.Core.Parsing;
using SproutDB.Core.Storage;

namespace SproutDB.Core.Execution;

internal static class CreateTableExecutor
{
    public static SproutResponse Execute(string query, string database, string dbPath, CreateTableQuery q, int chunkSize = StorageConstants.CHUNK_SIZE)
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

        // Create _index file (slot-based format with header)
        var indexPath = Path.Combine(tablePath, "_index");
        IndexHandle.CreateNew(indexPath, chunkSize);

        // Create .col files for each column
        foreach (var col in q.Columns)
        {
            CreatePreAllocatedFile(
                Path.Combine(tablePath, $"{col.Name}.col"),
                (long)chunkSize * col.EntrySize);
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

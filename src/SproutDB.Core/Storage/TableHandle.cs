namespace SproutDB.Core.Storage;

internal sealed class TableHandle : IDisposable
{
    private readonly string _tablePath;
    private readonly string _schemaPath;
    private readonly Dictionary<string, ColumnHandle> _columns = new();

    public TableSchema Schema { get; private set; }
    public IndexHandle Index { get; }

    private TableHandle(string tablePath, TableSchema schema, IndexHandle index)
    {
        _tablePath = tablePath;
        _schemaPath = Path.Combine(tablePath, "_schema.bin");
        Schema = schema;
        Index = index;
    }

    public static TableHandle Open(string tablePath)
    {
        var schemaPath = Path.Combine(tablePath, "_schema.bin");
        var schema = SchemaFile.Read(schemaPath);

        var indexPath = Path.Combine(tablePath, "_index");
        var index = new IndexHandle(indexPath);

        var handle = new TableHandle(tablePath, schema, index);

        // Open all column handles
        foreach (var col in schema.Columns)
        {
            var colPath = Path.Combine(tablePath, $"{col.Name}.col");
            handle._columns[col.Name] = new ColumnHandle(colPath, col);
        }

        return handle;
    }

    public ColumnHandle GetColumn(string name) => _columns[name];

    public bool HasColumn(string name) => _columns.ContainsKey(name);

    /// <summary>
    /// Adds a new column to this table (creates .col file, opens handle, updates schema).
    /// </summary>
    public void AddColumn(ColumnSchemaEntry entry)
    {
        Schema.Columns.Add(entry);

        // Create pre-allocated .col file
        var colPath = Path.Combine(_tablePath, $"{entry.Name}.col");
        using (var fs = File.Create(colPath))
        {
            fs.SetLength((long)StorageConstants.CHUNK_SIZE * entry.EntrySize);
        }

        // Open handle
        _columns[entry.Name] = new ColumnHandle(colPath, entry);
    }

    /// <summary>
    /// Replaces the schema entry for an existing column (type expansion).
    /// Reopens the column handle if entry size changed.
    /// </summary>
    public void UpdateColumnSchema(ColumnSchemaEntry updated)
    {
        var idx = Schema.Columns.FindIndex(c => c.Name == updated.Name);
        Schema.Columns[idx] = updated;

        // If entry size changed, reopen handle
        if (_columns.TryGetValue(updated.Name, out var existing) &&
            existing.Schema.EntrySize != updated.EntrySize)
        {
            existing.Dispose();
            var colPath = Path.Combine(_tablePath, $"{updated.Name}.col");
            _columns[updated.Name] = new ColumnHandle(colPath, updated);
        }
    }

    public void SaveSchema()
    {
        SchemaFile.Write(_schemaPath, Schema);
    }

    public void Dispose()
    {
        Index.Dispose();
        foreach (var col in _columns.Values)
            col.Dispose();
        _columns.Clear();
    }
}

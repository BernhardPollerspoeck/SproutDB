namespace SproutDB.Core.Storage;

internal sealed class TableHandle : IDisposable
{
    private readonly string _tablePath;
    private readonly string _schemaPath;
    private readonly Dictionary<string, ColumnHandle> _columns = [];

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

    /// <summary>
    /// Removes a column: disposes handle, deletes .col file, updates schema.
    /// </summary>
    public void RemoveColumn(string name)
    {
        if (_columns.TryGetValue(name, out var handle))
        {
            handle.Dispose();
            _columns.Remove(name);
        }

        var colPath = Path.Combine(_tablePath, $"{name}.col");
        if (File.Exists(colPath))
            File.Delete(colPath);

        Schema.Columns.RemoveAll(c => c.Name == name);
        SaveSchema();
    }

    /// <summary>
    /// Renames a column: disposes old handle, renames .col file, reopens handle, updates schema.
    /// </summary>
    public void RenameColumn(string oldName, string newName)
    {
        var oldPath = Path.Combine(_tablePath, $"{oldName}.col");
        var newPath = Path.Combine(_tablePath, $"{newName}.col");

        // Dispose old handle so file is released
        if (_columns.TryGetValue(oldName, out var handle))
        {
            handle.Dispose();
            _columns.Remove(oldName);
        }

        // Rename file on disk
        File.Move(oldPath, newPath);

        // Update schema entry
        var entry = Schema.Columns.Find(c => c.Name == oldName);
        if (entry is not null)
            entry.Name = newName;

        SaveSchema();

        // Reopen handle with new name
        if (entry is not null)
            _columns[newName] = new ColumnHandle(newPath, entry);
    }

    /// <summary>
    /// Rebuilds a string column with a new size.
    /// Creates temp file, copies data entry-by-entry (truncating if shrinking), swaps files.
    /// </summary>
    public void RebuildColumn(string name, int newSize)
    {
        var entry = Schema.Columns.Find(c => c.Name == name);
        if (entry is null) return;

        var oldSize = entry.Size;
        var oldEntrySize = entry.EntrySize;
        var newEntrySize = 1 + newSize; // flag byte + value
        var copySize = Math.Min(oldSize, newSize);

        var colPath = Path.Combine(_tablePath, $"{name}.col");
        var tmpPath = colPath + ".tmp";

        // Dispose old handle so file is released for reading
        if (_columns.TryGetValue(name, out var oldHandle))
        {
            oldHandle.Flush();
            oldHandle.Dispose();
            _columns.Remove(name);
        }

        // Build new file: read old file raw, write entries with new size
        using (var oldFs = new FileStream(colPath, FileMode.Open, FileAccess.Read, FileShare.Read))
        using (var newFs = File.Create(tmpPath))
        {
            var entryCount = oldFs.Length / oldEntrySize;
            newFs.SetLength(entryCount * newEntrySize);

            var oldBuf = new byte[oldEntrySize];
            var newBuf = new byte[newEntrySize];

            for (long i = 0; i < entryCount; i++)
            {
                oldFs.ReadExactly(oldBuf);
                Array.Clear(newBuf);

                // Copy flag byte
                newBuf[0] = oldBuf[0];

                // Copy value bytes (truncate if shrinking)
                Buffer.BlockCopy(oldBuf, 1, newBuf, 1, copySize);

                newFs.Write(newBuf);
            }
        }

        // Swap: delete old, rename tmp
        File.Delete(colPath);
        File.Move(tmpPath, colPath);

        // Update schema
        entry.Size = newSize;
        entry.EntrySize = newEntrySize;
        SaveSchema();

        // Reopen handle
        _columns[name] = new ColumnHandle(colPath, entry);
    }

    public void Flush()
    {
        Index.Flush();
        foreach (var col in _columns.Values)
            col.Flush();
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

namespace SproutDB.Core.Storage;

internal sealed class TableHandle : IDisposable
{
    private readonly string _tablePath;
    private readonly string _schemaPath;
    private readonly int _chunkSize;
    private readonly Dictionary<string, ColumnHandle> _columns = [];
    private readonly Dictionary<string, BTreeHandle> _btrees = [];

    public string TablePath => _tablePath;
    public TableSchema Schema { get; private set; }
    public IndexHandle Index { get; }
    public TtlHandle? Ttl { get; private set; }

    /// <summary>Whether this table has any TTL configured (table-level or row-level possible).</summary>
    public bool HasTtl => Ttl is not null;

    private TableHandle(string tablePath, TableSchema schema, IndexHandle index, TtlHandle? ttl, int chunkSize)
    {
        _tablePath = tablePath;
        _schemaPath = Path.Combine(tablePath, "_schema.bin");
        _chunkSize = chunkSize;
        Schema = schema;
        Index = index;
        Ttl = ttl;
    }

    public static TableHandle Open(string tablePath, int chunkSize = StorageConstants.CHUNK_SIZE)
    {
        var schemaPath = Path.Combine(tablePath, "_schema.bin");
        var schema = SchemaFile.Read(schemaPath);

        // Schema-level ChunkSize overrides caller default
        if (schema.ChunkSize > 0)
            chunkSize = schema.ChunkSize;

        var indexPath = Path.Combine(tablePath, "_index");
        var index = new IndexHandle(indexPath, chunkSize);

        // Open TTL handle if _ttl file exists
        TtlHandle? ttl = null;
        var ttlPath = Path.Combine(tablePath, "_ttl");
        if (File.Exists(ttlPath))
            ttl = new TtlHandle(ttlPath, chunkSize);

        var handle = new TableHandle(tablePath, schema, index, ttl, chunkSize);

        // Open all column handles + B-Trees
        foreach (var col in schema.Columns)
        {
            var colPath = Path.Combine(tablePath, $"{col.Name}.col");
            handle._columns[col.Name] = new ColumnHandle(colPath, col, chunkSize);

            var btreePath = Path.Combine(tablePath, $"{col.Name}.btree");
            if (File.Exists(btreePath))
            {
                ColumnTypes.TryParse(col.Type, out var colType);
                handle._btrees[col.Name] = BTreeHandle.Open(btreePath, colType, col.Size);
            }
        }

        return handle;
    }

    public ColumnHandle GetColumn(string name) => _columns[name];

    public bool HasColumn(string name) => _columns.ContainsKey(name);

    public int IndexCount => _btrees.Count;

    public IReadOnlyCollection<string> IndexedColumns => _btrees.Keys;

    public bool HasBTree(string colName) => _btrees.ContainsKey(colName);

    public BTreeHandle GetBTree(string colName) => _btrees[colName];

    public void AddBTree(string colName, BTreeHandle handle)
    {
        _btrees[colName] = handle;
    }

    public void RemoveBTree(string colName)
    {
        if (_btrees.TryGetValue(colName, out var handle))
        {
            handle.Dispose();
            _btrees.Remove(colName);
        }

        var btreePath = Path.Combine(_tablePath, $"{colName}.btree");
        if (File.Exists(btreePath))
            File.Delete(btreePath);
    }

    /// <summary>
    /// Rebuilds all B-Trees from source column data.
    /// Used to repair B-Trees that may have accumulated duplicate entries.
    /// </summary>
    public void RebuildAllBTrees()
    {
        foreach (var colName in _btrees.Keys.ToList())
        {
            var col = _columns[colName];
            var schema = col.Schema;
            ColumnTypes.TryParse(schema.Type, out var colType);

            _btrees[colName].Dispose();

            var btreePath = Path.Combine(_tablePath, $"{colName}.btree");
            File.Delete(btreePath);

            _btrees[colName] = BTreeHandle.BuildFromColumn(btreePath, col, Index, colType, schema.Size);
        }
    }

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
            fs.SetLength((long)_chunkSize * entry.EntrySize);
        }

        // Open handle
        _columns[entry.Name] = new ColumnHandle(colPath, entry, _chunkSize);
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

        // Remove B-Tree if exists
        RemoveBTree(name);

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

        // Rename B-Tree if exists
        if (_btrees.TryGetValue(oldName, out var btree))
        {
            btree.Dispose();
            _btrees.Remove(oldName);

            var oldBtreePath = Path.Combine(_tablePath, $"{oldName}.btree");
            var newBtreePath = Path.Combine(_tablePath, $"{newName}.btree");
            if (File.Exists(oldBtreePath))
            {
                File.Move(oldBtreePath, newBtreePath);
                if (entry is not null)
                {
                    ColumnTypes.TryParse(entry.Type, out var colType);
                    _btrees[newName] = BTreeHandle.Open(newBtreePath, colType, entry.Size);
                }
            }
        }
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

    /// <summary>
    /// Rebuilds a column file for type expansion (e.g. ubyte → ushort).
    /// Writes new .tmp file, migrates data, then safely swaps.
    /// </summary>
    public void RebuildColumnForTypeExpansion(
        ColumnSchemaEntry entry, ColumnType newType, int newSize, int newEntrySize,
        List<(long place, string raw)> values)
    {
        var colPath = Path.Combine(_tablePath, $"{entry.Name}.col");
        var tmpPath = colPath + ".tmp";

        // Dispose old handle so file is released
        if (_columns.TryGetValue(entry.Name, out var oldHandle))
        {
            oldHandle.Flush();
            oldHandle.Dispose();
            _columns.Remove(entry.Name);
        }

        // Determine entry count from old file
        var oldEntrySize = entry.EntrySize;
        var oldFileLength = new FileInfo(colPath).Length;
        var entryCount = oldFileLength / oldEntrySize;

        // Create temporary schema entry for new handle
        var tmpEntry = new ColumnSchemaEntry
        {
            Name = entry.Name,
            Type = ColumnTypes.GetName(newType),
            Size = newSize,
            EntrySize = newEntrySize,
            Nullable = entry.Nullable,
            Default = entry.Default,
            Strict = entry.Strict,
            IsUnique = entry.IsUnique,
        };

        // Create new .tmp file with correct capacity
        using (var fs = File.Create(tmpPath))
        {
            fs.SetLength(entryCount * newEntrySize);
        }

        // Open handle on .tmp with new type, write all values
        var tmpHandle = new ColumnHandle(tmpPath, tmpEntry, _chunkSize);
        foreach (var (place, raw) in values)
        {
            tmpHandle.WriteValue(place, raw);
        }
        tmpHandle.Flush();
        tmpHandle.Dispose();

        // Safe swap: delete old, rename tmp
        File.Delete(colPath);
        File.Move(tmpPath, colPath);

        // Update schema entry in-place
        entry.Type = tmpEntry.Type;
        entry.Size = newSize;
        entry.EntrySize = newEntrySize;

        // Remove B-Tree if exists (encoded values changed size)
        RemoveBTree(entry.Name);

        // Reopen handle on final file
        _columns[entry.Name] = new ColumnHandle(colPath, entry, _chunkSize);
    }

    // ── Blob file helpers ────────────────────────────────────

    public string GetBlobPath(string columnName, long id)
        => Path.Combine(_tablePath, $"{columnName}_{id}.blob");

    public void WriteBlobFile(string columnName, long id, byte[] data)
    {
        var path = GetBlobPath(columnName, id);
        File.WriteAllBytes(path, data);
    }

    public byte[] ReadBlobFile(string columnName, long id)
    {
        var path = GetBlobPath(columnName, id);
        return File.ReadAllBytes(path);
    }

    public void DeleteBlobFile(string columnName, long id)
    {
        var path = GetBlobPath(columnName, id);
        if (File.Exists(path))
            File.Delete(path);
    }

    // ── Array file helpers ───────────────────────────────────

    public string GetArrayPath(string columnName, long id)
        => Path.Combine(_tablePath, $"{columnName}_{id}.array");

    public void WriteArrayFile(string columnName, long id, byte[] data)
    {
        var path = GetArrayPath(columnName, id);
        File.WriteAllBytes(path, data);
    }

    public byte[] ReadArrayFile(string columnName, long id)
    {
        var path = GetArrayPath(columnName, id);
        return File.ReadAllBytes(path);
    }

    public void DeleteArrayFile(string columnName, long id)
    {
        var path = GetArrayPath(columnName, id);
        if (File.Exists(path))
            File.Delete(path);
    }

    public long GetStorageSizeBytes()
    {
        long total = 0;
        foreach (var file in Directory.EnumerateFiles(_tablePath))
            total += new FileInfo(file).Length;
        return total;
    }

    /// <summary>
    /// Creates and opens the _ttl file for this table.
    /// </summary>
    public void EnableTtl()
    {
        if (Ttl is not null) return;
        var ttlPath = Path.Combine(_tablePath, "_ttl");
        TtlHandle.CreateNew(ttlPath, _chunkSize);
        Ttl = new TtlHandle(ttlPath, _chunkSize);
    }

    public void Flush()
    {
        Index.Flush();
        Ttl?.Flush();
        foreach (var col in _columns.Values)
            col.Flush();
        foreach (var btree in _btrees.Values)
            btree.Flush();
    }

    public void SaveSchema()
    {
        SchemaFile.Write(_schemaPath, Schema);
    }

    public void Dispose()
    {
        Index.Dispose();
        Ttl?.Dispose();
        foreach (var col in _columns.Values)
            col.Dispose();
        _columns.Clear();
        foreach (var btree in _btrees.Values)
            btree.Dispose();
        _btrees.Clear();
    }
}

namespace SproutDB.Core.Storage;

internal sealed class TableHandle : IDisposable
{
    private readonly string _tablePath;
    private readonly string _schemaPath;
    private readonly int _chunkSize;
    private readonly Dictionary<string, Lazy<ColumnHandle>> _columns = [];
    private readonly Dictionary<string, Lazy<BTreeHandle>> _btrees = [];

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

        // Register columns + B-Trees lazily — each handle opens its MMF only
        // when first accessed. Keeps a cold table at ~2 FDs (index + optional
        // ttl) regardless of column/index count.
        foreach (var col in schema.Columns)
            handle.RegisterColumn(col);

        return handle;
    }

    private void RegisterColumn(ColumnSchemaEntry col)
    {
        var colPath = Path.Combine(_tablePath, $"{col.Name}.col");
        var colEntry = col;
        var chunkSize = _chunkSize;
        _columns[col.Name] = new Lazy<ColumnHandle>(
            () => new ColumnHandle(colPath, colEntry, chunkSize));

        var btreePath = Path.Combine(_tablePath, $"{col.Name}.btree");
        if (File.Exists(btreePath))
        {
            _btrees[col.Name] = new Lazy<BTreeHandle>(() =>
            {
                ColumnTypes.TryParse(colEntry.Type, out var colType);
                return BTreeHandle.Open(btreePath, colType, colEntry.Size);
            });
        }
    }

    public ColumnHandle GetColumn(string name) => _columns[name].Value;

    public bool HasColumn(string name) => _columns.ContainsKey(name);

    public int IndexCount => _btrees.Count;

    public IReadOnlyCollection<string> IndexedColumns => _btrees.Keys;

    public bool HasBTree(string colName) => _btrees.ContainsKey(colName);

    public BTreeHandle GetBTree(string colName) => _btrees[colName].Value;

    public void AddBTree(string colName, BTreeHandle handle)
    {
        // The caller hands us an already-open handle. Wrap it in a Lazy that is
        // immediately materialized so Dispose/Flush see IsValueCreated == true
        // and actually close the file.
        var captured = handle;
        var lazy = new Lazy<BTreeHandle>(() => captured);
        _ = lazy.Value;
        _btrees[colName] = lazy;
    }

    public void RemoveBTree(string colName)
    {
        if (_btrees.TryGetValue(colName, out var lazy))
        {
            if (lazy.IsValueCreated)
                lazy.Value.Dispose();
            _btrees.Remove(colName);
        }

        var btreePath = Path.Combine(_tablePath, $"{colName}.btree");
        if (File.Exists(btreePath))
            File.Delete(btreePath);
    }

    /// <summary>
    /// Adds a new column to this table (creates .col file, registers handle, updates schema).
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

        RegisterColumn(entry);
    }

    /// <summary>
    /// Replaces the schema entry for an existing column (type expansion).
    /// Re-registers the column handle if entry size changed.
    /// </summary>
    public void UpdateColumnSchema(ColumnSchemaEntry updated)
    {
        var idx = Schema.Columns.FindIndex(c => c.Name == updated.Name);
        Schema.Columns[idx] = updated;

        // If entry size changed, re-register (existing handle becomes stale)
        if (_columns.TryGetValue(updated.Name, out var existing)
            && existing.IsValueCreated
            && existing.Value.Schema.EntrySize != updated.EntrySize)
        {
            existing.Value.Dispose();
        }

        // Rebuild the Lazy factory with the new schema entry
        var colPath = Path.Combine(_tablePath, $"{updated.Name}.col");
        _columns[updated.Name] = new Lazy<ColumnHandle>(
            () => new ColumnHandle(colPath, updated));
    }

    /// <summary>
    /// Removes a column: disposes handle if opened, deletes .col file, updates schema.
    /// </summary>
    public void RemoveColumn(string name)
    {
        if (_columns.TryGetValue(name, out var lazy))
        {
            if (lazy.IsValueCreated)
                lazy.Value.Dispose();
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
    /// Renames a column: disposes old handle if open, renames .col file,
    /// re-registers handle, updates schema.
    /// </summary>
    public void RenameColumn(string oldName, string newName)
    {
        var oldPath = Path.Combine(_tablePath, $"{oldName}.col");
        var newPath = Path.Combine(_tablePath, $"{newName}.col");

        if (_columns.TryGetValue(oldName, out var lazy))
        {
            if (lazy.IsValueCreated)
                lazy.Value.Dispose();
            _columns.Remove(oldName);
        }

        File.Move(oldPath, newPath);

        var entry = Schema.Columns.Find(c => c.Name == oldName);
        if (entry is not null)
            entry.Name = newName;

        SaveSchema();

        if (entry is not null)
        {
            var capturedEntry = entry;
            _columns[newName] = new Lazy<ColumnHandle>(
                () => new ColumnHandle(newPath, capturedEntry));
        }

        // Rename B-Tree if exists
        if (_btrees.TryGetValue(oldName, out var btreeLazy))
        {
            if (btreeLazy.IsValueCreated)
                btreeLazy.Value.Dispose();
            _btrees.Remove(oldName);

            var oldBtreePath = Path.Combine(_tablePath, $"{oldName}.btree");
            var newBtreePath = Path.Combine(_tablePath, $"{newName}.btree");
            if (File.Exists(oldBtreePath))
            {
                File.Move(oldBtreePath, newBtreePath);
                if (entry is not null)
                {
                    var capturedEntry = entry;
                    _btrees[newName] = new Lazy<BTreeHandle>(() =>
                    {
                        ColumnTypes.TryParse(capturedEntry.Type, out var colType);
                        return BTreeHandle.Open(newBtreePath, colType, capturedEntry.Size);
                    });
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

        // Dispose old handle if it had been opened
        if (_columns.TryGetValue(name, out var lazy))
        {
            if (lazy.IsValueCreated)
            {
                lazy.Value.Flush();
                lazy.Value.Dispose();
            }
            _columns.Remove(name);
        }

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

                newBuf[0] = oldBuf[0];
                Buffer.BlockCopy(oldBuf, 1, newBuf, 1, copySize);

                newFs.Write(newBuf);
            }
        }

        File.Delete(colPath);
        File.Move(tmpPath, colPath);

        entry.Size = newSize;
        entry.EntrySize = newEntrySize;
        SaveSchema();

        var capturedEntry = entry;
        _columns[name] = new Lazy<ColumnHandle>(() => new ColumnHandle(colPath, capturedEntry));
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

        if (_columns.TryGetValue(entry.Name, out var lazy))
        {
            if (lazy.IsValueCreated)
            {
                lazy.Value.Flush();
                lazy.Value.Dispose();
            }
            _columns.Remove(entry.Name);
        }

        var oldEntrySize = entry.EntrySize;
        var oldFileLength = new FileInfo(colPath).Length;
        var entryCount = oldFileLength / oldEntrySize;

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

        using (var fs = File.Create(tmpPath))
        {
            fs.SetLength(entryCount * newEntrySize);
        }

        // Temporarily open a handle on .tmp to write migrated values
        var tmpHandle = new ColumnHandle(tmpPath, tmpEntry, _chunkSize);
        foreach (var (place, raw) in values)
        {
            tmpHandle.WriteValue(place, raw);
        }
        tmpHandle.Flush();
        tmpHandle.Dispose();

        File.Delete(colPath);
        File.Move(tmpPath, colPath);

        entry.Type = tmpEntry.Type;
        entry.Size = newSize;
        entry.EntrySize = newEntrySize;

        // Remove B-Tree if exists (encoded values changed size)
        RemoveBTree(entry.Name);

        var capturedEntry = entry;
        var chunkSize = _chunkSize;
        _columns[entry.Name] = new Lazy<ColumnHandle>(
            () => new ColumnHandle(colPath, capturedEntry, chunkSize));
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
        foreach (var lazy in _columns.Values)
        {
            if (lazy.IsValueCreated)
                lazy.Value.Flush();
        }
        foreach (var lazy in _btrees.Values)
        {
            if (lazy.IsValueCreated)
                lazy.Value.Flush();
        }
    }

    public void SaveSchema()
    {
        SchemaFile.Write(_schemaPath, Schema);
    }

    public void Dispose()
    {
        Index.Dispose();
        Ttl?.Dispose();
        foreach (var lazy in _columns.Values)
        {
            if (lazy.IsValueCreated)
                lazy.Value.Dispose();
        }
        _columns.Clear();
        foreach (var lazy in _btrees.Values)
        {
            if (lazy.IsValueCreated)
                lazy.Value.Dispose();
        }
        _btrees.Clear();
    }
}

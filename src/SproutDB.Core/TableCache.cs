using System.Collections.Concurrent;
using SproutDB.Core.Storage;

namespace SproutDB.Core;

internal sealed class TableCache : IDisposable
{
    private readonly ConcurrentDictionary<string, Lazy<TableHandle>> _tables = new();
    private readonly ConcurrentDictionary<string, byte> _knownDatabases = new();
    private readonly int _chunkSize;

    public TableCache(int chunkSize = StorageConstants.CHUNK_SIZE)
    {
        _chunkSize = chunkSize;
    }

    public TableHandle GetOrOpen(string tablePath)
    {
        var chunkSize = _chunkSize;
        var lazy = _tables.GetOrAdd(tablePath,
            path => new Lazy<TableHandle>(() => TableHandle.Open(path, chunkSize)));
        return lazy.Value;
    }

    public bool TryGetTable(string tablePath, out TableHandle? table)
    {
        if (_tables.TryGetValue(tablePath, out var lazy) && lazy.IsValueCreated)
        {
            table = lazy.Value;
            return true;
        }
        table = null;
        return false;
    }

    public bool DatabaseExists(string dbPath)
    {
        if (_knownDatabases.ContainsKey(dbPath))
            return true;

        if (!Directory.Exists(dbPath))
            return false;

        _knownDatabases[dbPath] = 0;
        return true;
    }

    public void RegisterDatabase(string dbPath)
    {
        _knownDatabases[dbPath] = 0;
    }

    public void UnregisterDatabase(string dbPath)
    {
        _knownDatabases.TryRemove(dbPath, out _);
    }

    public void OpenTablesForDatabase(string dbPath)
    {
        foreach (var tableDir in Directory.GetDirectories(dbPath))
        {
            var schemaPath = Path.Combine(tableDir, "_schema.bin");
            if (!File.Exists(schemaPath))
                continue;

            GetOrOpen(tableDir);
        }
    }

    public void FlushTablesForDatabase(string dbPath)
    {
        foreach (var (path, lazy) in _tables)
        {
            if (lazy.IsValueCreated && path.StartsWith(dbPath, StringComparison.Ordinal))
                lazy.Value.Flush();
        }
    }

    public void FlushAll()
    {
        foreach (var lazy in _tables.Values)
        {
            if (lazy.IsValueCreated)
                lazy.Value.Flush();
        }
    }

    public void EvictTable(string tablePath)
    {
        if (_tables.TryRemove(tablePath, out var lazy) && lazy.IsValueCreated)
            lazy.Value.Dispose();
    }

    public void EvictTablesForDatabase(string dbPath)
    {
        var prefix = dbPath + Path.DirectorySeparatorChar;
        foreach (var (path, lazy) in _tables)
        {
            if (path.StartsWith(prefix, StringComparison.Ordinal))
            {
                if (_tables.TryRemove(path, out _) && lazy.IsValueCreated)
                    lazy.Value.Dispose();
            }
        }
    }

    /// <summary>
    /// Returns all currently opened table handles (only tables that have been loaded into memory).
    /// </summary>
    public IEnumerable<(string Path, TableHandle Table)> GetAllOpened()
    {
        foreach (var (path, lazy) in _tables)
        {
            if (lazy.IsValueCreated)
                yield return (path, lazy.Value);
        }
    }

    public void Dispose()
    {
        foreach (var lazy in _tables.Values)
        {
            if (lazy.IsValueCreated)
                lazy.Value.Dispose();
        }
        _tables.Clear();
    }
}

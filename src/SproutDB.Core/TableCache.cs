using System.Collections.Concurrent;
using SproutDB.Core.Storage;

namespace SproutDB.Core;

internal sealed class TableCache : IDisposable
{
    private readonly ConcurrentDictionary<string, Lazy<TableHandle>> _tables = new();
    private readonly ConcurrentDictionary<string, long> _lastAccessTicks = new();
    private readonly ConcurrentDictionary<string, byte> _knownDatabases = new();
    private readonly int _chunkSize;
    private int _maxOpenTables;
    private Func<string, bool>? _dbBusyChecker;

    public TableCache(int chunkSize = StorageConstants.CHUNK_SIZE)
    {
        _chunkSize = chunkSize;
    }

    /// <summary>
    /// Sets the soft cap on open tables and the callback used to decide whether
    /// a table's database is currently under an active lease (in which case the
    /// table must not be evicted mid-query). Called once by the engine during
    /// construction, after the scope manager is ready.
    /// </summary>
    public void ConfigureEviction(int maxOpenTables, Func<string, bool> dbBusyChecker)
    {
        _maxOpenTables = maxOpenTables;
        _dbBusyChecker = dbBusyChecker;
    }

    public TableHandle GetOrOpen(string tablePath)
    {
        var chunkSize = _chunkSize;
        var lazy = _tables.GetOrAdd(tablePath,
            path => new Lazy<TableHandle>(() => TableHandle.Open(path, chunkSize)));
        _lastAccessTicks[tablePath] = Environment.TickCount64;
        var handle = lazy.Value;

        EnforceMaxOpenTables(except: tablePath);
        return handle;
    }

    public bool TryGetTable(string tablePath, out TableHandle? table)
    {
        if (_tables.TryGetValue(tablePath, out var lazy) && lazy.IsValueCreated)
        {
            _lastAccessTicks[tablePath] = Environment.TickCount64;
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
        _lastAccessTicks.TryRemove(tablePath, out _);
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
                _lastAccessTicks.TryRemove(path, out _);
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

    /// <summary>
    /// Returns the number of tables currently held in the cache
    /// (including those with non-materialized lazy handles).
    /// </summary>
    public int CachedCount => _tables.Count;

    /// <summary>
    /// Returns the number of tables that have their MMFs actually open.
    /// </summary>
    public int OpenedCount
    {
        get
        {
            var count = 0;
            foreach (var lazy in _tables.Values)
                if (lazy.IsValueCreated)
                    count++;
            return count;
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
        _lastAccessTicks.Clear();
    }

    // ── Cap enforcement ─────────────────────────────────────

    /// <summary>
    /// If the cache exceeds <see cref="SproutEngineSettings.MaxOpenTables"/>,
    /// evicts the oldest table(s) whose database is not under an active lease.
    /// Tables belonging to busy databases are skipped — evicting them would
    /// race with in-flight queries. <paramref name="except"/> is never evicted.
    /// </summary>
    private void EnforceMaxOpenTables(string except)
    {
        var cap = _maxOpenTables;
        if (cap <= 0 || _tables.Count <= cap) return;

        var checker = _dbBusyChecker;
        if (checker is null) return;

        var candidates = new List<(string Path, long Ticks)>();
        foreach (var (path, lazy) in _tables)
        {
            if (path == except) continue;
            if (!lazy.IsValueCreated) continue;

            var dbPath = Path.GetDirectoryName(path);
            if (dbPath is null) continue;
            if (checker(dbPath)) continue; // DB under lease — keep its tables

            var ticks = _lastAccessTicks.TryGetValue(path, out var t) ? t : 0;
            candidates.Add((path, ticks));
        }

        if (candidates.Count == 0) return;

        candidates.Sort((a, b) => a.Ticks.CompareTo(b.Ticks));
        var excess = _tables.Count - cap;
        var evictCount = Math.Min(excess, candidates.Count);
        for (var i = 0; i < evictCount; i++)
            EvictTable(candidates[i].Path);
    }
}

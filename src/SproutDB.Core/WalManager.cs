using System.Collections.Concurrent;
using SproutDB.Core.Storage;

namespace SproutDB.Core;

internal sealed class WalManager : IDisposable
{
    private readonly ConcurrentDictionary<string, WalFile> _wals = new();
    private readonly ConcurrentDictionary<string, object> _locks = new();

    private object GetLock(string dbPath) => _locks.GetOrAdd(dbPath, _ => new object());

    public WalFile GetOrOpen(string dbPath)
    {
        if (_wals.TryGetValue(dbPath, out var existing))
            return existing;

        // Serialize open vs evict for this path so an in-flight Dispose
        // cannot race an Open on the same underlying _wal file.
        lock (GetLock(dbPath))
        {
            if (_wals.TryGetValue(dbPath, out existing))
                return existing;

            var wal = new WalFile(Path.Combine(dbPath, "_wal"));
            _wals[dbPath] = wal;
            return wal;
        }
    }

    public void SyncAll()
    {
        // ConcurrentDictionary.Values returns a snapshot; WalFile.SyncToDisk
        // is disposed-safe, so a concurrent Evict is harmless here.
        foreach (var wal in _wals.Values)
            wal.SyncToDisk();
    }

    public void TruncateAll()
    {
        foreach (var wal in _wals.Values)
        {
            if (!wal.IsEmpty)
                wal.Truncate();
        }
    }

    public long GetTotalSizeBytes()
    {
        long total = 0;
        foreach (var wal in _wals.Values)
            total += wal.SizeBytes;
        return total;
    }

    public void Evict(string dbPath)
    {
        lock (GetLock(dbPath))
        {
            if (_wals.TryRemove(dbPath, out var wal))
                wal.Dispose();
        }
    }

    public void Dispose()
    {
        foreach (var wal in _wals.Values)
            wal.Dispose();
        _wals.Clear();
        _locks.Clear();
    }
}

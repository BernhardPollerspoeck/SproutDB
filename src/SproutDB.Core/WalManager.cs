using SproutDB.Core.Storage;

namespace SproutDB.Core;

internal sealed class WalManager : IDisposable
{
    private readonly Dictionary<string, WalFile> _wals = [];

    public WalFile GetOrOpen(string dbPath)
    {
        if (!_wals.TryGetValue(dbPath, out var wal))
        {
            var walPath = Path.Combine(dbPath, "_wal");
            wal = new WalFile(walPath);
            _wals[dbPath] = wal;
        }
        return wal;
    }

    public void SyncAll()
    {
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

    public void Evict(string dbPath)
    {
        if (_wals.TryGetValue(dbPath, out var wal))
        {
            wal.Dispose();
            _wals.Remove(dbPath);
        }
    }

    public void Dispose()
    {
        foreach (var wal in _wals.Values)
            wal.Dispose();
        _wals.Clear();
    }
}

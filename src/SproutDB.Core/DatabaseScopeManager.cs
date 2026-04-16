using System.Collections.Concurrent;

namespace SproutDB.Core;

/// <summary>
/// Tracks per-database lifetime state (last-access, in-flight refcount, pin
/// flag) and coordinates eviction of <see cref="WalManager"/> +
/// <see cref="TableCache"/> entries. Three eviction triggers:
/// <list type="bullet">
///   <item>Idle sweep: runs on a timer every
///     <see cref="SproutEngineSettings.IdleEvictInterval"/>, closes databases
///     idle for &gt; <see cref="SproutEngineSettings.IdleEvictAfterSeconds"/>.</item>
///   <item>Cap enforcement: every <see cref="Acquire"/> evicts the oldest
///     non-busy database when the open count is at or above
///     <see cref="SproutEngineSettings.MaxOpenDatabases"/>. Soft limit —
///     all-busy never blocks.</item>
///   <item>Memory pressure: a <see cref="Gen2GcCallback"/> checks the GC's
///     memory-load ratio and halves the number of open databases when the
///     threshold is exceeded.</item>
/// </list>
/// Pinned scopes (e.g. <c>_system</c>) are excluded from all three.
/// </summary>
internal sealed class DatabaseScopeManager : IDisposable
{
    private readonly WalManager _walManager;
    private readonly TableCache _tableCache;
    private readonly SproutEngineSettings _settings;
    private readonly ConcurrentDictionary<string, DbState> _states = new();
    private readonly Timer? _idleTimer;
    private volatile bool _disposed;

    public DatabaseScopeManager(
        WalManager walManager,
        TableCache tableCache,
        SproutEngineSettings settings)
    {
        _walManager = walManager;
        _tableCache = tableCache;
        _settings = settings;

        if (settings.IdleEvictInterval != Timeout.InfiniteTimeSpan
            && settings.IdleEvictInterval > TimeSpan.Zero)
        {
            _idleTimer = new Timer(
                static s => ((DatabaseScopeManager)s!).RunIdleSweep(),
                this,
                settings.IdleEvictInterval,
                settings.IdleEvictInterval);
        }

        if (settings.EnableMemoryPressureEviction)
        {
            Gen2GcCallback.Register(static target =>
                ((DatabaseScopeManager)target).OnGen2Gc(),
                this);
        }
    }

    /// <summary>
    /// Acquires a lease on the scope for <paramref name="dbPath"/>. The lease
    /// blocks eviction of this database for its lifetime. Always release via
    /// <c>using</c>.
    /// </summary>
    public Lease Acquire(string dbPath)
    {
        var state = AcquireInternal(dbPath);
        EnforceMaxOpen(except: dbPath);
        return new Lease(this, dbPath, state);
    }

    /// <summary>
    /// Marks <paramref name="dbPath"/> as pinned — it will never be evicted
    /// by idle, cap, or memory-pressure triggers. Intended for
    /// <c>_system</c>.
    /// </summary>
    public void Pin(string dbPath)
    {
        var state = _states.GetOrAdd(dbPath, _ => new DbState());
        lock (state.Lock)
        {
            state.Pinned = true;
        }
    }

    /// <summary>
    /// Removes tracking for <paramref name="dbPath"/>. Called from
    /// purge/restore flows.
    /// </summary>
    public void UnregisterDatabase(string dbPath)
    {
        _states.TryRemove(dbPath, out _);
    }

    // ── Inspection (tests + metrics) ─────────────────────────

    public int OpenDatabaseCount => _states.Count;

    public bool IsOpen(string dbPath) => _states.ContainsKey(dbPath);

    internal int GetRefCount(string dbPath)
        => _states.TryGetValue(dbPath, out var s) ? Volatile.Read(ref s.RefCount) : 0;

    // ── Eviction entry points ────────────────────────────────

    /// <summary>
    /// Evicts scopes whose <c>LastAccessTicks &lt; cutoffTicks</c> and are not
    /// busy or pinned. Public for tests + timer callback.
    /// </summary>
    public void EvictIdle(long cutoffTicks)
    {
        if (_disposed) return;

        foreach (var (dbPath, state) in _states)
        {
            if (state.Pinned) continue;
            if (Volatile.Read(ref state.RefCount) > 0) continue;
            if (Volatile.Read(ref state.LastAccessTicks) >= cutoffTicks) continue;

            TryEvict(dbPath, state);
        }
    }

    /// <summary>
    /// Evicts up to half of the currently open, non-pinned, non-busy scopes,
    /// oldest first. Called from Gen2-GC callback when the process is under
    /// memory pressure.
    /// </summary>
    public void EvictOnMemoryPressure()
    {
        if (_disposed) return;

        var candidates = new List<(string Path, DbState State, long LastAccess)>();
        foreach (var (dbPath, state) in _states)
        {
            if (state.Pinned) continue;
            if (Volatile.Read(ref state.RefCount) > 0) continue;
            candidates.Add((dbPath, state, Volatile.Read(ref state.LastAccessTicks)));
        }

        if (candidates.Count == 0) return;

        candidates.Sort((a, b) => a.LastAccess.CompareTo(b.LastAccess));
        var toEvict = (candidates.Count + 1) / 2;

        for (var i = 0; i < toEvict; i++)
            TryEvict(candidates[i].Path, candidates[i].State);
    }

    // ── Internals ────────────────────────────────────────────

    private DbState AcquireInternal(string dbPath)
    {
        while (true)
        {
            var state = _states.GetOrAdd(dbPath, _ => new DbState());
            lock (state.Lock)
            {
                // Verify we got the same state that's still tracked. If the
                // state was evicted between GetOrAdd and lock-acquire, it
                // won't be in _states anymore — retry with a fresh one.
                if (!_states.TryGetValue(dbPath, out var current) || !ReferenceEquals(current, state))
                    continue;

                state.RefCount++;
                state.LastAccessTicks = Environment.TickCount64;
                return state;
            }
        }
    }

    internal void Release(DbState state)
    {
        lock (state.Lock)
        {
            state.RefCount--;
            state.LastAccessTicks = Environment.TickCount64;
        }
    }

    private void EnforceMaxOpen(string except)
    {
        var cap = _settings.MaxOpenDatabases;
        if (cap <= 0 || _states.Count <= cap) return;

        // Find oldest non-busy non-pinned candidates (exclude the one we just acquired)
        var candidates = new List<(string Path, DbState State, long LastAccess)>();
        foreach (var (dbPath, state) in _states)
        {
            if (dbPath == except) continue;
            if (state.Pinned) continue;
            if (Volatile.Read(ref state.RefCount) > 0) continue;
            candidates.Add((dbPath, state, Volatile.Read(ref state.LastAccessTicks)));
        }

        if (candidates.Count == 0) return; // all busy — soft override

        candidates.Sort((a, b) => a.LastAccess.CompareTo(b.LastAccess));

        var excess = _states.Count - cap;
        for (var i = 0; i < Math.Min(excess, candidates.Count); i++)
            TryEvict(candidates[i].Path, candidates[i].State);
    }

    private void TryEvict(string dbPath, DbState state)
    {
        lock (state.Lock)
        {
            if (state.Pinned) return;
            if (state.RefCount > 0) return;

            // Still safe — do the actual handle release while holding the lock
            // so no concurrent Acquire can observe a half-evicted state.
            try
            {
                _walManager.Evict(dbPath);
                _tableCache.EvictTablesForDatabase(dbPath);
            }
            finally
            {
                _states.TryRemove(dbPath, out _);
            }
        }
    }

    private void RunIdleSweep()
    {
        if (_disposed) return;

        var idleMs = _settings.IdleEvictAfterSeconds * 1000L;
        if (idleMs <= 0) return;

        var cutoff = Environment.TickCount64 - idleMs;
        EvictIdle(cutoff);
    }

    private bool OnGen2Gc()
    {
        if (_disposed) return false;

        try
        {
            var info = GC.GetGCMemoryInfo();
            if (info.HighMemoryLoadThresholdBytes > 0)
            {
                var loadPct = (info.MemoryLoadBytes * 100.0) / info.HighMemoryLoadThresholdBytes;
                if (loadPct >= _settings.MemoryPressureThresholdPercent)
                    EvictOnMemoryPressure();
            }
        }
        catch
        {
            // Never throw from a GC callback — swallow and stay registered
        }

        return true; // stay registered
    }

    public void Dispose()
    {
        if (_disposed) return;
        _disposed = true;
        _idleTimer?.Dispose();
        _states.Clear();
    }

    // ── Types ────────────────────────────────────────────────

    internal sealed class DbState
    {
        public readonly object Lock = new();
        public int RefCount;
        public long LastAccessTicks;
        public bool Pinned;
    }

    public sealed class Lease : IDisposable
    {
        private readonly DatabaseScopeManager _manager;
        private readonly DbState _state;
        private int _disposed;

        public string DbPath { get; }

        internal Lease(DatabaseScopeManager manager, string dbPath, DbState state)
        {
            _manager = manager;
            DbPath = dbPath;
            _state = state;
        }

        public void Dispose()
        {
            if (Interlocked.Exchange(ref _disposed, 1) != 0) return;
            _manager.Release(_state);
        }
    }
}

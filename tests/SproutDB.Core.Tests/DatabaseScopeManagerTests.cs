using SproutDB.Core.Storage;

namespace SproutDB.Core.Tests;

public class DatabaseScopeManagerTests : IDisposable
{
    private readonly string _tempDir;
    private readonly WalManager _walManager = new();
    private readonly TableCache _tableCache = new();
    private readonly SproutEngineSettings _settings;
    private readonly DatabaseScopeManager _scopes;

    public DatabaseScopeManagerTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), $"sproutdb-scope-{Guid.NewGuid()}");
        Directory.CreateDirectory(_tempDir);
        _settings = new SproutEngineSettings
        {
            DataDirectory = _tempDir,
            IdleEvictAfterSeconds = 300,
            MaxOpenDatabases = 128,
            EnableMemoryPressureEviction = true,
            MemoryPressureThresholdPercent = 80,
        };
        _scopes = new DatabaseScopeManager(_walManager, _tableCache, _settings);
    }

    public void Dispose()
    {
        _scopes.Dispose();
        _walManager.Dispose();
        _tableCache.Dispose();
        if (Directory.Exists(_tempDir))
            Directory.Delete(_tempDir, true);
    }

    private string EnsureDb(string name)
    {
        var path = Path.Combine(_tempDir, name);
        Directory.CreateDirectory(path);
        _tableCache.RegisterDatabase(path);
        return path;
    }

    // ── Lifecycle ────────────────────────────────────────────

    [Fact]
    public void Acquire_CreatesScopeAndTracksIt()
    {
        var dbPath = EnsureDb("db1");

        using (var lease = _scopes.Acquire(dbPath))
        {
            Assert.Equal(1, _scopes.OpenDatabaseCount);
            Assert.Equal(dbPath, lease.DbPath);
        }
    }

    [Fact]
    public void Acquire_SameDb_ReusesScope_And_Nests()
    {
        var dbPath = EnsureDb("db1");

        using var l1 = _scopes.Acquire(dbPath);
        using var l2 = _scopes.Acquire(dbPath);

        Assert.Equal(1, _scopes.OpenDatabaseCount);
        Assert.Equal(2, _scopes.GetRefCount(dbPath));
    }

    [Fact]
    public void Dispose_Lease_DecrementsRefCount()
    {
        var dbPath = EnsureDb("db1");

        var l1 = _scopes.Acquire(dbPath);
        Assert.Equal(1, _scopes.GetRefCount(dbPath));
        l1.Dispose();
        Assert.Equal(0, _scopes.GetRefCount(dbPath));
    }

    // ── Pinning ──────────────────────────────────────────────

    [Fact]
    public void Pinned_Scope_NotEvictedOnIdle()
    {
        var dbPath = EnsureDb("_system");
        _scopes.Pin(dbPath);

        using (_scopes.Acquire(dbPath)) { /* touch */ }

        // Force idle-evict with cutoff far in the future (everything "idle")
        _scopes.EvictIdle(cutoffTicks: long.MaxValue);

        Assert.Equal(1, _scopes.OpenDatabaseCount);
    }

    [Fact]
    public void Unpinned_Scope_EvictedOnIdle()
    {
        var dbPath = EnsureDb("db1");

        using (_scopes.Acquire(dbPath)) { /* touch */ }

        _scopes.EvictIdle(cutoffTicks: long.MaxValue);

        Assert.Equal(0, _scopes.OpenDatabaseCount);
    }

    [Fact]
    public void EvictIdle_SkipsBusyScopes()
    {
        var dbPath = EnsureDb("db1");

        using var lease = _scopes.Acquire(dbPath);

        _scopes.EvictIdle(cutoffTicks: long.MaxValue);

        // Busy → still open even though "idle"
        Assert.Equal(1, _scopes.OpenDatabaseCount);
    }

    [Fact]
    public void EvictIdle_SkipsRecentlyAccessedScopes()
    {
        var dbPath = EnsureDb("db1");

        using (_scopes.Acquire(dbPath)) { /* touch = now */ }

        // Cutoff BEFORE access → scope is "recent"
        _scopes.EvictIdle(cutoffTicks: 0);

        Assert.Equal(1, _scopes.OpenDatabaseCount);
    }

    // ── Cap enforcement ──────────────────────────────────────

    [Fact]
    public void Acquire_BeyondCap_EvictsOldestNonBusy()
    {
        var settings = new SproutEngineSettings
        {
            DataDirectory = _tempDir,
            IdleEvictAfterSeconds = 300,
            MaxOpenDatabases = 3,
        };
        using var walMgr = new WalManager();
        using var tableCache = new TableCache();
        using var scopes = new DatabaseScopeManager(walMgr, tableCache, settings);

        var p1 = EnsurePath("a"); tableCache.RegisterDatabase(p1);
        var p2 = EnsurePath("b"); tableCache.RegisterDatabase(p2);
        var p3 = EnsurePath("c"); tableCache.RegisterDatabase(p3);
        var p4 = EnsurePath("d"); tableCache.RegisterDatabase(p4);

        using (scopes.Acquire(p1)) { }
        Thread.Sleep(5);
        using (scopes.Acquire(p2)) { }
        Thread.Sleep(5);
        using (scopes.Acquire(p3)) { }
        Thread.Sleep(5);

        // All three held → count=3, cap=3
        Assert.Equal(3, scopes.OpenDatabaseCount);

        // Fourth acquire → oldest (p1) must be evicted
        using (scopes.Acquire(p4)) { }

        Assert.Equal(3, scopes.OpenDatabaseCount);
        Assert.False(scopes.IsOpen(p1));
        Assert.True(scopes.IsOpen(p4));
    }

    [Fact]
    public void Acquire_BeyondCap_AllBusy_AllowsSoftOverride()
    {
        var settings = new SproutEngineSettings
        {
            DataDirectory = _tempDir,
            IdleEvictAfterSeconds = 300,
            MaxOpenDatabases = 2,
        };
        using var walMgr = new WalManager();
        using var tableCache = new TableCache();
        using var scopes = new DatabaseScopeManager(walMgr, tableCache, settings);

        var p1 = EnsurePath("a"); tableCache.RegisterDatabase(p1);
        var p2 = EnsurePath("b"); tableCache.RegisterDatabase(p2);
        var p3 = EnsurePath("c"); tableCache.RegisterDatabase(p3);

        using var l1 = scopes.Acquire(p1);
        using var l2 = scopes.Acquire(p2);

        // Both busy, cap=2 → soft override, no block
        using var l3 = scopes.Acquire(p3);

        Assert.Equal(3, scopes.OpenDatabaseCount);  // over cap, soft
    }

    // ── Memory-pressure eviction ─────────────────────────────

    [Fact]
    public void EvictOnMemoryPressure_HalvesOpenDatabases()
    {
        var paths = new List<string>();
        for (var i = 0; i < 10; i++)
        {
            var p = EnsurePath($"mem_{i}");
            _tableCache.RegisterDatabase(p);
            paths.Add(p);
            using (_scopes.Acquire(p)) { }
            Thread.Sleep(2);
        }
        Assert.Equal(10, _scopes.OpenDatabaseCount);

        _scopes.EvictOnMemoryPressure();

        // Halved (5 evicted, oldest non-busy first)
        Assert.InRange(_scopes.OpenDatabaseCount, 4, 6);
    }

    [Fact]
    public void EvictOnMemoryPressure_SkipsPinned()
    {
        var pinned = EnsureDb("_system");
        _scopes.Pin(pinned);
        using (_scopes.Acquire(pinned)) { }

        for (var i = 0; i < 6; i++)
        {
            var p = EnsurePath($"t_{i}");
            _tableCache.RegisterDatabase(p);
            using (_scopes.Acquire(p)) { }
            Thread.Sleep(2);
        }

        _scopes.EvictOnMemoryPressure();

        Assert.True(_scopes.IsOpen(pinned));
    }

    // ── Concurrency ──────────────────────────────────────────

    [Fact]
    public void Acquire_Evict_Race_NoCorruption()
    {
        var paths = new List<string>();
        for (var i = 0; i < 20; i++)
        {
            var p = EnsurePath($"race_{i}");
            _tableCache.RegisterDatabase(p);
            paths.Add(p);
        }

        var stop = DateTime.UtcNow.AddSeconds(1);
        var threads = new List<Thread>();

        // Acquirers
        for (var t = 0; t < 4; t++)
        {
            threads.Add(new Thread(() =>
            {
                var rng = new Random();
                while (DateTime.UtcNow < stop)
                {
                    var p = paths[rng.Next(paths.Count)];
                    using (_scopes.Acquire(p)) { }
                }
            }));
        }

        // Evictor
        threads.Add(new Thread(() =>
        {
            while (DateTime.UtcNow < stop)
            {
                _scopes.EvictIdle(cutoffTicks: long.MaxValue);
                Thread.Sleep(1);
            }
        }));

        foreach (var th in threads) th.Start();
        foreach (var th in threads) th.Join();

        // No assertion on final count — just no exceptions/corruption
        Assert.True(_scopes.OpenDatabaseCount >= 0);
    }

    private string EnsurePath(string name)
    {
        var p = Path.Combine(_tempDir, name);
        Directory.CreateDirectory(p);
        return p;
    }
}

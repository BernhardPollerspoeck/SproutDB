namespace SproutDB.Core.Tests;

/// <summary>
/// The TableCache LRU cap is the per-table counterpart to
/// <see cref="SproutEngineSettings.MaxOpenDatabases"/>. Its job: once a
/// process has accumulated more open tables than the cap allows, evict the
/// oldest ones from databases that are not currently under a lease.
/// Busy databases are protected so in-flight queries never race a dispose.
/// </summary>
public class TableCacheLruTests : IDisposable
{
    private readonly string _tempDir;

    public TableCacheLruTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), $"sproutdb-tablelru-{Guid.NewGuid()}");
    }

    public void Dispose()
    {
        if (Directory.Exists(_tempDir))
            Directory.Delete(_tempDir, true);
    }

    [Fact]
    public void OpeningManyTables_BeyondCap_EvictsOldestFromIdleDbs()
    {
        var settings = new SproutEngineSettings
        {
            DataDirectory = _tempDir,
            MaxOpenTables = 8,
            MaxOpenDatabases = 128,
            IdleEvictInterval = Timeout.InfiniteTimeSpan,
        };
        using var engine = new SproutEngine(settings);

        // Each DB has 3 tables. After touching 5 DBs (15 tables total), cap=8
        // should force evictions — but all idle DBs are candidates.
        for (int i = 0; i < 5; i++)
        {
            var db = engine.GetOrCreateDatabase($"tenant_{i}");
            db.QueryOne("create table t1 (v sint)");
            db.QueryOne("create table t2 (v sint)");
            db.QueryOne("create table t3 (v sint)");
            db.QueryOne("upsert t1 {v: 1}");
            db.QueryOne("upsert t2 {v: 1}");
            db.QueryOne("upsert t3 {v: 1}");
        }

        // At this point each DB has been fully touched but none is under a lease.
        // Cap should have kept things bounded. Allow a small overshoot because
        // opening happens before the enforce step returns.
        var opened = engine.OpenedTableCount;
        Assert.True(opened <= settings.MaxOpenTables + 3,
            $"OpenedTableCount={opened}, expected ≤ {settings.MaxOpenTables + 3}");
    }

    [Fact]
    public void EvictionSkipsTables_InBusyDatabases()
    {
        var settings = new SproutEngineSettings
        {
            DataDirectory = _tempDir,
            MaxOpenTables = 2,
            MaxOpenDatabases = 128,
            IdleEvictInterval = Timeout.InfiniteTimeSpan,
        };
        using var engine = new SproutEngine(settings);

        var pinnedDb = engine.GetOrCreateDatabase("pinned");
        pinnedDb.QueryOne("create table keep_me (v sint)");
        pinnedDb.QueryOne("upsert keep_me {v: 1}");

        // Hold a lease on pinnedDb while we bombard the cache from other DBs.
        // The engine's internal scope manager sees RefCount > 0 for pinned and
        // refuses to evict keep_me even though it's the oldest.
        using var leaseHolder = Task.Run(async () =>
        {
            for (int i = 0; i < 200; i++)
            {
                pinnedDb.QueryOne("get keep_me");
                await Task.Delay(1);
            }
        });

        // While the lease bursts run, open tables in many other DBs.
        for (int i = 0; i < 10; i++)
        {
            var db = engine.GetOrCreateDatabase($"churn_{i}");
            db.QueryOne("create table t (v sint)");
            db.QueryOne("upsert t {v: 1}");
        }

        leaseHolder.Wait();

        // keep_me must still be readable and return 1 row.
        var r = pinnedDb.QueryOne("get keep_me");
        Assert.Single(r.Data ?? []);
    }

    [Fact]
    public void DisabledCap_ZeroMaxOpenTables_NeverEvicts()
    {
        var settings = new SproutEngineSettings
        {
            DataDirectory = _tempDir,
            MaxOpenTables = 0, // disabled
            MaxOpenDatabases = 128,
            IdleEvictInterval = Timeout.InfiniteTimeSpan,
        };
        using var engine = new SproutEngine(settings);

        for (int i = 0; i < 3; i++)
        {
            var db = engine.GetOrCreateDatabase($"t_{i}");
            db.QueryOne("create table a (v sint)");
            db.QueryOne("create table b (v sint)");
            db.QueryOne("upsert a {v: 1}");
            db.QueryOne("upsert b {v: 1}");
        }

        Assert.True(engine.OpenedTableCount >= 6);
    }
}

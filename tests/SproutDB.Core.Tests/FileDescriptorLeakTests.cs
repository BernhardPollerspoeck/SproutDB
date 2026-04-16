using System.Diagnostics;
using System.Runtime.InteropServices;
using SproutDB.Core.Storage;
using Xunit.Abstractions;

namespace SproutDB.Core.Tests;

/// <summary>
/// Reproducers for the FD-leak reported in the field:
///
///   System.IO.IOException: No file descriptors available : '/data/sproutdb/&lt;db&gt;/_meta.bin'
///     at SproutDB.Core.Storage.MetaFile.Read
///     at SproutDB.Core.SproutEngine.ResolveChunkSize
///     at SproutDB.Core.SproutEngine.ExecuteCreateTable
///
/// Suspected sources: MetaFile.Read leaking in exception paths, or MMF handles
/// accumulating across repeated provision-calls.
/// </summary>
public class FileDescriptorLeakTests : IDisposable
{
    private readonly string _tempDir;
    private readonly SproutEngine _engine;
    private readonly ITestOutputHelper _log;

    public FileDescriptorLeakTests(ITestOutputHelper log)
    {
        _log = log;
        _tempDir = Path.Combine(Path.GetTempPath(), $"sproutdb-fdtest-{Guid.NewGuid()}");
        _engine = new SproutEngine(_tempDir);
    }

    public void Dispose()
    {
        _engine.Dispose();
        if (Directory.Exists(_tempDir))
            Directory.Delete(_tempDir, true);
    }

    /// <summary>
    /// MetaFile.Read is called from ResolveChunkSize on every CreateTable.
    /// It must not leak handles even under many repeated calls.
    /// </summary>
    [Fact]
    public void MetaFile_Read_DoesNotLeakHandles()
    {
        // Arrange: create a real _meta.bin to read
        var dir = Path.Combine(_tempDir, "metaleak");
        Directory.CreateDirectory(dir);
        var metaPath = Path.Combine(dir, "_meta.bin");
        MetaFile.Write(metaPath, DateTime.UtcNow.Ticks, chunkSize: 1024);

        // Warm up (JIT, first allocations)
        for (var i = 0; i < 100; i++)
            MetaFile.Read(metaPath);

        var before = SampleHandleCount();

        // Act: many reads
        const int iterations = 5000;
        for (var i = 0; i < iterations; i++)
            MetaFile.Read(metaPath);

        var after = SampleHandleCount();
        var delta = after - before;
        _log.WriteLine($"MetaFile.Read: before={before} after={after} delta={delta} over {iterations} iterations");

        // Assert: each iteration opens + closes one FileStream.
        // A small drift (<50) is acceptable; anywhere near `iterations` is a leak.
        Assert.True(delta < 200,
            $"MetaFile.Read appears to leak handles: before={before} after={after} delta={delta} over {iterations} iterations");
    }

    /// <summary>
    /// Repeated CreateTable in the same DB calls ResolveChunkSize → MetaFile.Read
    /// on every invocation. The TableHandle is NOT opened by CreateTable itself
    /// (lazy via TableCache), so handle growth should be bounded to roughly one
    /// open TableHandle per table — not proportional to the number of calls.
    /// </summary>
    [Fact]
    public void CreateTable_Repeated_DoesNotLeakHandles()
    {
        var db = _engine.GetOrCreateDatabase("fdleak");

        // Create a baseline table so TableCache is warm
        db.QueryOne("create table warmup (val sint)");

        for (var i = 0; i < 50; i++)
            db.QueryOne($"create table t{i} (val sint, name string 64)");

        var before = SampleHandleCount();

        // Act: many more CreateTable calls.
        // Each new table creates files on disk but does not open TableHandle.
        const int iterations = 200;
        for (var i = 50; i < 50 + iterations; i++)
            db.QueryOne($"create table t{i} (val sint, name string 64)");

        var after = SampleHandleCount();
        var delta = after - before;
        _log.WriteLine($"CreateTable x{iterations}: before={before} after={after} delta={delta}");

        // Each CreateTable creates ~3-4 files (schema + index + col + optional ttl).
        // File.Create uses `using` in CreatePreAllocatedFile, so no persistent handles.
        // Drift should be bounded — definitely not `iterations * columnCount`.
        Assert.True(delta < 500,
            $"CreateTable appears to leak handles: before={before} after={after} delta={delta} over {iterations} iterations");
    }

    /// <summary>
    /// Creating + using many tables in a single DB accumulates TableHandles in
    /// the cache (by design — MMF + column handles stay open for fast access).
    /// But each TableHandle should hold a bounded number of OS handles, roughly
    /// proportional to (columns + btrees + 2). This test measures the per-table
    /// cost so a regression in TableHandle.Open would show up as a jump.
    /// </summary>
    [Fact]
    public void TableHandle_PerTable_HandleCost_IsBounded()
    {
        var db = _engine.GetOrCreateDatabase("percost");

        // Baseline: create + touch one table so TableCache path is warm
        db.QueryOne("create table seed (val sint)");
        db.QueryOne("get seed");

        var before = SampleHandleCount();

        const int tables = 20;
        for (var i = 0; i < tables; i++)
        {
            db.QueryOne($"create table t{i} (a sint, b sint, c string 64)");
            // Touch the table so TableHandle is actually opened
            db.QueryOne($"get t{i}");
        }

        var after = SampleHandleCount();
        var delta = after - before;
        var perTable = delta / (double)tables;
        _log.WriteLine($"TableHandle per-table cost: before={before} after={after} delta={delta} tables={tables} perTable={perTable:F1}");

        // With 3 columns and no btrees, each TableHandle opens roughly:
        // _schema (closed after read) + _index (MMF, 2 handles) + 3 cols (MMF, 2 each)
        // → ~8-10 handles per table is reasonable.
        // If we see > 50 per table, something leaks per-column or per-index.
        Assert.True(perTable < 50,
            $"Per-table handle cost is too high: {perTable:F1} handles/table (before={before}, after={after}, tables={tables})");
    }

    /// <summary>
    /// The reported crash was on a host named `navio_tenant_&lt;name&gt;` — i.e. a
    /// multi-tenant deployment with ONE SproutDB per tenant. Every tenant DB
    /// holds at least:
    ///   - 1 WAL file handle (WalManager.GetOrOpen — never evicted)
    ///   - N TableHandles (each with MMFs for _index + each column + btrees)
    /// Nothing ever closes these. With ulimit 1024 + enough tenants, the server
    /// hits EMFILE and crashes in ResolveChunkSize → MetaFile.Read.
    ///
    /// This test provisions many tiny tenant DBs (migration-style: one
    /// _migrations-like table each) and measures handle growth. It documents
    /// the per-tenant handle cost so a regression shows up.
    /// </summary>
    [Fact]
    public void ManyTenantDatabases_HandleGrowth_IsBounded()
    {
        // Baseline warm-up
        var warmup = _engine.GetOrCreateDatabase("warmup");
        warmup.QueryOne("create table seed (val sint)");
        warmup.QueryOne("upsert seed {val: 1}");

        var before = SampleHandleCount();

        const int tenants = 50;
        for (var i = 0; i < tenants; i++)
        {
            var db = _engine.GetOrCreateDatabase($"tenant_{i}");
            db.QueryOne("create table migrations_like (name string 64, ord sint)");
            db.QueryOne("upsert migrations_like {name: 'init', ord: 1}");
        }

        var after = SampleHandleCount();
        var delta = after - before;
        var perTenant = delta / (double)tenants;
        _log.WriteLine($"Multi-tenant DBs: before={before} after={after} delta={delta} tenants={tenants} perTenant={perTenant:F1}");
        _log.WriteLine($"Extrapolated for 1024 FD ulimit: {1024 / Math.Max(perTenant, 0.1):F0} tenants before EMFILE");

        // Per tenant we expect: WAL + 1 TableHandle (index + 2 cols) ≈ ~10 handles.
        // If this jumps far above that, either TableCache or WalManager is
        // double-opening, or something new was added to the open path.
        // Default ulimit 1024 means a threshold of ~20 handles/tenant caps us
        // at ~50 tenants — which is exactly what the production crash shows.
        Assert.True(perTenant < 30,
            $"Per-tenant DB handle cost is too high: {perTenant:F1} handles/tenant " +
            $"(before={before}, after={after}, tenants={tenants}). " +
            $"This is the documented multi-tenant FD exhaustion.");
    }

    /// <summary>
    /// Triggers idle-evict with an aggressive short timeout (via custom engine
    /// settings) and verifies that the handle count drops back toward baseline
    /// after Tenant-DBs go idle. This is the fix for the original report.
    /// </summary>
    [Fact]
    public void IdleEviction_ReclaimsHandles_FromIdleTenantDatabases()
    {
        // Separate engine with aggressive idle-evict for the test
        var tempDir = Path.Combine(Path.GetTempPath(), $"sproutdb-idle-{Guid.NewGuid()}");
        var settings = new SproutEngineSettings
        {
            DataDirectory = tempDir,
            IdleEvictAfterSeconds = 1,
            IdleEvictInterval = TimeSpan.FromMilliseconds(200),
            MaxOpenDatabases = 128,
        };
        using var engine = new SproutEngine(settings);

        var warmup = engine.GetOrCreateDatabase("warmup");
        warmup.QueryOne("create table seed (val sint)");
        warmup.QueryOne("upsert seed {val: 1}");

        var baseline = SampleHandleCount();

        const int tenants = 30;
        for (var i = 0; i < tenants; i++)
        {
            var db = engine.GetOrCreateDatabase($"idle_tenant_{i}");
            db.QueryOne("create table t (name string 64, ord sint)");
            db.QueryOne("upsert t {name: 'x', ord: 1}");
        }

        var peak = SampleHandleCount();
        var peakDelta = peak - baseline;
        _log.WriteLine($"Idle-evict test: baseline={baseline} peak={peak} peakDelta={peakDelta} (after opening {tenants} tenants)");

        // Wait long enough for:
        //   - IdleEvictAfterSeconds (1s)
        //   - IdleEvictInterval (200ms) to fire at least once past that
        Thread.Sleep(TimeSpan.FromSeconds(2.5));

        var after = SampleHandleCount();
        var delta = after - baseline;
        _log.WriteLine($"After idle-evict sleep: after={after} delta={delta} (was peak={peak})");

        // After idle-evict, most tenant handles must be released.
        // We expect delta < peakDelta / 2 — i.e. at least half the handles reclaimed.
        Assert.True(delta < peakDelta / 2,
            $"Idle-evict did not reclaim enough handles: peakDelta={peakDelta}, afterDelta={delta}");
    }

    /// <summary>
    /// With idle-evict enabled at 1 second, provisioning 500 tenant DBs in
    /// sequence must not blow past the MaxOpenDatabases cap and must not hit
    /// EMFILE. Each tenant touched once, then moves on.
    /// </summary>
    [Fact]
    public void ManyTenants_WithCap_StaysBounded()
    {
        var tempDir = Path.Combine(Path.GetTempPath(), $"sproutdb-cap-{Guid.NewGuid()}");
        var settings = new SproutEngineSettings
        {
            DataDirectory = tempDir,
            IdleEvictAfterSeconds = 1,
            IdleEvictInterval = TimeSpan.FromMilliseconds(200),
            MaxOpenDatabases = 32,
        };
        using var engine = new SproutEngine(settings);

        var baseline = SampleHandleCount();

        const int tenants = 500;
        for (var i = 0; i < tenants; i++)
        {
            var db = engine.GetOrCreateDatabase($"cap_tenant_{i}");
            db.QueryOne("create table t (name string 64)");
            db.QueryOne("upsert t {name: 'x'}");
        }

        var after = SampleHandleCount();
        var delta = after - baseline;
        _log.WriteLine($"Cap test: baseline={baseline} after={after} delta={delta} over {tenants} tenants (cap=32)");

        // With cap=32 and typical ~7 FDs per tenant → ~224 expected peak.
        // We allow 500 as a generous ceiling (handles incl. GC/JIT drift).
        Assert.True(delta < 500,
            $"Cap enforcement failed: delta={delta} over {tenants} tenants suggests unbounded growth");
    }

    // ── Helpers ──────────────────────────────────────────────────────

    private static int SampleHandleCount()
    {
        // Force GC so orphaned MMFs / FileStreams have their finalizers run.
        // A true leak survives finalization; a false positive (in-flight GC) does not.
        GC.Collect();
        GC.WaitForPendingFinalizers();
        GC.Collect();

        if (RuntimeInformation.IsOSPlatform(OSPlatform.Linux))
        {
            var fdDir = "/proc/self/fd";
            if (Directory.Exists(fdDir))
                return Directory.GetFileSystemEntries(fdDir).Length;
        }

        using var proc = Process.GetCurrentProcess();
        proc.Refresh();
        return proc.HandleCount;
    }
}

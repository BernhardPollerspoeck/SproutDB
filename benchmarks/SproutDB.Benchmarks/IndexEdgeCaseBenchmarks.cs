using BenchmarkDotNet.Attributes;
using SproutDB.Core;

namespace SproutDB.Benchmarks;

/// <summary>
/// Benchmarks for index edge cases that the new slot-based index aims to improve.
/// Run BEFORE and AFTER the slot-index migration to compare.
/// </summary>
[MemoryDiagnoser]
[ShortRunJob]
public class IndexEdgeCaseBenchmarks
{
    private string _tempDir = null!;
    private SproutEngine _engine = null!;

    [GlobalSetup]
    public void Setup()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), $"sproutdb-bench-idx-{Guid.NewGuid()}");
        _engine = new SproutEngine(new SproutEngineSettings
        {
            DataDirectory = _tempDir,
            FlushInterval = Timeout.InfiniteTimeSpan,
        });

        _engine.Execute("create database", "bench");
        _engine.Execute(
            "create table users (name string 100, email string 320, age ubyte, score sint)",
            "bench");
    }

    [GlobalCleanup]
    public void Cleanup()
    {
        _engine.Dispose();
        if (Directory.Exists(_tempDir))
            Directory.Delete(_tempDir, true);
    }

    // ── Scenario 1: GET on fresh table (baseline) ──────────────

    [Benchmark(Description = "1a: GET 20K rows (fresh, no deletes)")]
    public SproutResponse Get_Fresh_20K()
    {
        ResetTable(20_000);
        return _engine.Execute("get users", "bench")[0];
    }

    [Benchmark(Description = "1b: GET 100K rows (fresh, no deletes)")]
    public SproutResponse Get_Fresh_100K()
    {
        ResetTable(100_000);
        return _engine.Execute("get users", "bench")[0];
    }

    // ── Scenario 2: GET after mass delete (fragmentation) ──────

    [Benchmark(Description = "2a: GET 2K alive after 18K deleted (of 20K)")]
    public SproutResponse Get_AfterMassDelete_20K_90pct()
    {
        ResetTable(20_000);
        // Delete 90% — IDs 1..18000
        _engine.Execute("delete users where _id <= 18000", "bench");
        return _engine.Execute("get users", "bench")[0];
    }

    [Benchmark(Description = "2b: GET 10K alive after 90K deleted (of 100K)")]
    public SproutResponse Get_AfterMassDelete_100K_90pct()
    {
        ResetTable(100_000);
        _engine.Execute("delete users where _id <= 90000", "bench");
        return _engine.Execute("get users", "bench")[0];
    }

    // ── Scenario 3: GET after mass delete + re-insert ──────────

    [Benchmark(Description = "3: GET 20K after delete 18K + re-insert 18K")]
    public SproutResponse Get_AfterDeleteAndReinsert_20K()
    {
        ResetTable(20_000);
        _engine.Execute("delete users where _id <= 18000", "bench");
        // Re-insert 18K rows (reuses freed places)
        for (var i = 0; i < 18_000; i++)
            _engine.Execute($"upsert users {{name: 'Refill{i}', age: {i % 100}, score: {i}}}", "bench");
        return _engine.Execute("get users", "bench")[0];
    }

    // ── Scenario 4: Delete by _id (ID→Place lookup) ───────────

    [Benchmark(Description = "4a: delete by _id in 20K table")]
    public SproutResponse Delete_ById_20K()
    {
        ResetTable(20_000);
        return _engine.Execute("delete users where _id = 10000", "bench")[0];
    }

    [Benchmark(Description = "4b: delete by _id in 100K table")]
    public SproutResponse Delete_ById_100K()
    {
        ResetTable(100_000);
        return _engine.Execute("delete users where _id = 50000", "bench")[0];
    }

    // ── Scenario 5: Insert throughput after mass delete ────────

    [Benchmark(Description = "5: 1000 inserts after 90% deleted (place reuse)")]
    public void Insert_AfterMassDelete()
    {
        ResetTable(20_000);
        _engine.Execute("delete users where _id <= 18000", "bench");
        for (var i = 0; i < 1_000; i++)
            _engine.Execute($"upsert users {{name: 'New{i}', age: 25, score: {i}}}", "bench");
    }

    // ── Scenario 6: GET with WHERE after mass delete ───────────

    [Benchmark(Description = "6: GET where score > 19000 after 90% deleted (of 20K)")]
    public SproutResponse Get_WithWhere_AfterMassDelete()
    {
        ResetTable(20_000);
        _engine.Execute("delete users where _id <= 18000", "bench");
        return _engine.Execute("get users where score > 19000", "bench")[0];
    }

    // ── Scenario 7: Scattered deletes (non-contiguous gaps) ───

    [Benchmark(Description = "7a: GET after 50% scattered deletes (every 2nd row) 20K")]
    public SproutResponse Get_ScatteredDelete_50pct_20K()
    {
        ResetTable(20_000);
        // Delete every even ID — creates maximum fragmentation
        _engine.Execute("delete users where _id % 2 = 0", "bench");
        return _engine.Execute("get users", "bench")[0];
    }

    [Benchmark(Description = "7b: GET after 50% scattered + 5K re-insert (backfill pattern)")]
    public SproutResponse Get_ScatteredDelete_ThenReinsert()
    {
        ResetTable(20_000);
        _engine.Execute("delete users where _id % 2 = 0", "bench");
        // Re-insert — these go into freed places (FIFO now, backfill later)
        for (var i = 0; i < 5_000; i++)
            _engine.Execute($"upsert users {{name: 'Backfill{i}', age: 30, score: {i}}}", "bench");
        return _engine.Execute("get users", "bench")[0];
    }

    // ── Scenario 8: Multiple delete+insert cycles (churn) ─────

    [Benchmark(Description = "8: 3 cycles of delete 50% + re-insert (churn)")]
    public SproutResponse Get_AfterChurn()
    {
        ResetTable(10_000);
        for (var cycle = 0; cycle < 3; cycle++)
        {
            // Delete half
            _engine.Execute($"delete users where score < {5000 + cycle * 1000}", "bench");
            // Re-insert
            for (var i = 0; i < 3_000; i++)
                _engine.Execute($"upsert users {{name: 'Churn{cycle}_{i}', age: 25, score: {50000 + cycle * 10000 + i}}}", "bench");
        }
        return _engine.Execute("get users", "bench")[0];
    }

    // ── Scenario 9: Full purge + rebuild (worst case backfill) ─

    [Benchmark(Description = "9: GET 10K after full purge + 10K re-insert")]
    public SproutResponse Get_AfterFullPurgeAndRebuild()
    {
        ResetTable(10_000);
        _engine.Execute("delete users where _id > 0", "bench");
        // All 10K places are free — inserts reuse them all
        for (var i = 0; i < 10_000; i++)
            _engine.Execute($"upsert users {{name: 'Rebuild{i}', age: {i % 100}, score: {i}}}", "bench");
        return _engine.Execute("get users", "bench")[0];
    }

    // ── Scenario 10: Table open time (ScanMaxPlace cost) ──────

    [Benchmark(Description = "10: Table open with 20K rows + 18K gaps")]
    public void TableOpen_WithGaps()
    {
        // This measures the cost of ScanMaxPlace() / free-place rebuild at open
        ResetTable(20_000);
        _engine.Execute("delete users where _id <= 18000", "bench");
        // Force table close + reopen by purging and recreating the engine
        // Not possible without engine restart — instead measure GET which includes the scan overhead
        _engine.Execute("get users", "bench");
    }

    // ── Helper ─────────────────────────────────────────────────

    private void ResetTable(int rowCount)
    {
        // Drop and recreate to ensure clean state
        _engine.Execute("purge table users", "bench");
        _engine.Execute(
            "create table users (name string 100, email string 320, age ubyte, score sint)",
            "bench");

        for (var i = 0; i < rowCount; i++)
            _engine.Execute($"upsert users {{name: 'User{i}', email: 'user{i}@test.com', age: {i % 100}, score: {i}}}", "bench");
    }
}

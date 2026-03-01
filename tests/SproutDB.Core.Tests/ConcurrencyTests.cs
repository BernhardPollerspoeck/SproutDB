using System.Collections.Concurrent;

namespace SproutDB.Core.Tests;

public class ConcurrencyTests : IDisposable
{
    private readonly string _tempDir;

    public ConcurrencyTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), $"sproutdb-conc-test-{Guid.NewGuid()}");
        Directory.CreateDirectory(_tempDir);
    }

    public void Dispose()
    {
        if (Directory.Exists(_tempDir))
            Directory.Delete(_tempDir, true);
    }

    private SproutEngine CreateEngine(string? subDir = null)
    {
        var dir = subDir is null ? _tempDir : Path.Combine(_tempDir, subDir);
        return new SproutEngine(new SproutEngineSettings
        {
            DataDirectory = dir,
            FlushInterval = Timeout.InfiniteTimeSpan,
            WalSyncInterval = TimeSpan.Zero,
        });
    }

    private static void SetupDbAndTable(SproutEngine engine, string db = "testdb")
    {
        engine.Execute("create database", db);
        engine.Execute("create table users (name string 100, score sint)", db);
    }

    // ── 1. Parallel reads see consistent data ────────────────

    [Fact]
    public void ParallelReads_SeeConsistentData()
    {
        using var engine = CreateEngine("parallel-reads");
        SetupDbAndTable(engine);

        for (int i = 1; i <= 50; i++)
            engine.Execute($"upsert users {{name: 'User{i}', score: {i * 10}}}", "testdb");

        var exceptions = new ConcurrentBag<Exception>();
        var tasks = new Task[10];

        for (int t = 0; t < tasks.Length; t++)
        {
            tasks[t] = Task.Run(() =>
            {
                try
                {
                    for (int r = 0; r < 20; r++)
                    {
                        var result = engine.Execute("get users", "testdb");
                        Assert.Equal(SproutOperation.Get, result.Operation);
                        Assert.Equal(50, result.Data?.Count);
                    }
                }
                catch (Exception ex)
                {
                    exceptions.Add(ex);
                }
            });
        }

        Task.WaitAll(tasks);

        if (!exceptions.IsEmpty)
            throw new AggregateException(exceptions);
    }

    // ── 2. Parallel writes: all IDs unique, no losses ────────

    [Fact]
    public void ParallelWrites_AllIdsUnique_NoLosses()
    {
        using var engine = CreateEngine("parallel-writes");
        SetupDbAndTable(engine);

        const int writersCount = 8;
        const int writesPerThread = 25;
        var tasks = new Task[writersCount];

        for (int t = 0; t < writersCount; t++)
        {
            var threadId = t;
            tasks[t] = Task.Run(() =>
            {
                for (int w = 0; w < writesPerThread; w++)
                {
                    var r = engine.Execute(
                        $"upsert users {{name: 'T{threadId}W{w}', score: {threadId * 100 + w}}}",
                        "testdb");
                    Assert.Null(r.Errors);
                    Assert.Equal(SproutOperation.Upsert, r.Operation);
                }
            });
        }

        Task.WaitAll(tasks);

        var totalExpected = writersCount * writesPerThread;

        // Use count to verify total (auto-paging would truncate Data)
        var countResult = engine.Execute("get users count", "testdb");
        Assert.Equal(totalExpected, countResult.Affected);

        // Collect all IDs via paging
        var allIds = new List<ulong>();
        var page = 1;
        while (true)
        {
            var result = engine.Execute($"get users select _id page {page} size 100", "testdb");
            if (result.Data is null || result.Data.Count == 0) break;
            allIds.AddRange(result.Data.Select(row => (ulong)row["_id"]!));
            if (result.Paging?.Next is null) break;
            page++;
        }

        Assert.Equal(totalExpected, allIds.Count);
        Assert.Equal(totalExpected, allIds.Distinct().Count());
    }

    // ── 3. Reads during writes: no crash, no torn rows ──────

    [Fact]
    public void ReadsDuringWrites_NoCrash()
    {
        using var engine = CreateEngine("mixed-rw");
        SetupDbAndTable(engine);

        // Seed some initial data
        for (int i = 0; i < 10; i++)
            engine.Execute($"upsert users {{name: 'Seed{i}', score: {i}}}", "testdb");

        var cts = new CancellationTokenSource(TimeSpan.FromSeconds(3));
        var exceptions = new ConcurrentBag<Exception>();

        // Writer thread
        var writerTask = Task.Run(() =>
        {
            int counter = 100;
            while (!cts.Token.IsCancellationRequested)
            {
                try
                {
                    engine.Execute($"upsert users {{name: 'W{counter}', score: {counter}}}", "testdb");
                    counter++;
                }
                catch (Exception ex)
                {
                    exceptions.Add(ex);
                }
            }
        });

        // Reader threads
        var readerTasks = Enumerable.Range(0, 4).Select(_ => Task.Run(() =>
        {
            while (!cts.Token.IsCancellationRequested)
            {
                try
                {
                    var r = engine.Execute("get users", "testdb");
                    Assert.Equal(SproutOperation.Get, r.Operation);
                    Assert.NotNull(r.Data);
                    Assert.True(r.Data.Count >= 10);
                }
                catch (Exception ex)
                {
                    exceptions.Add(ex);
                }
            }
        })).ToArray();

        Task.WaitAll([writerTask, .. readerTasks]);

        if (!exceptions.IsEmpty)
            throw new AggregateException(exceptions);
    }

    // ── 4. After restart: parallel reads work immediately ────

    [Fact]
    public void AfterRestart_ParallelReadsWork()
    {
        var dataDir = Path.Combine(_tempDir, "restart-reads");

        // First engine: create and populate
        using (var engine = new SproutEngine(new SproutEngineSettings
        {
            DataDirectory = dataDir,
            FlushInterval = Timeout.InfiniteTimeSpan,
            WalSyncInterval = TimeSpan.Zero,
        }))
        {
            SetupDbAndTable(engine);
            for (int i = 1; i <= 20; i++)
                engine.Execute($"upsert users {{name: 'User{i}', score: {i}}}", "testdb");
        }

        // Second engine: parallel reads after replay
        using (var engine = new SproutEngine(new SproutEngineSettings
        {
            DataDirectory = dataDir,
            FlushInterval = Timeout.InfiniteTimeSpan,
            WalSyncInterval = TimeSpan.Zero,
        }))
        {
            var tasks = Enumerable.Range(0, 6).Select(_ => Task.Run(() =>
            {
                var r = engine.Execute("get users", "testdb");
                Assert.Equal(SproutOperation.Get, r.Operation);
                Assert.Equal(20, r.Data?.Count);
            })).ToArray();

            Task.WaitAll(tasks);
        }
    }

    // ── 5. FlushCycle + aggressive writes: no corruption ─────

    [Fact]
    public void FlushCycleWithWrites_NoCorruption()
    {
        var dataDir = Path.Combine(_tempDir, "flush-writes");

        // Use a short flush interval to exercise the flush path
        using var engine = new SproutEngine(new SproutEngineSettings
        {
            DataDirectory = dataDir,
            FlushInterval = TimeSpan.FromMilliseconds(50),
            WalSyncInterval = TimeSpan.FromMilliseconds(10),
        });

        SetupDbAndTable(engine);

        const int totalWrites = 100;
        var tasks = Enumerable.Range(0, 4).Select(t => Task.Run(() =>
        {
            for (int w = 0; w < totalWrites / 4; w++)
                engine.Execute($"upsert users {{name: 'F{t}W{w}', score: {t * 100 + w}}}", "testdb");
        })).ToArray();

        Task.WaitAll(tasks);

        // Wait a bit to let flush cycles run
        Thread.Sleep(200);

        var result = engine.Execute("get users select _id, name", "testdb");
        Assert.Equal(totalWrites, result.Data?.Count);

        var ids = result.Data?.Select(row => (ulong)row["_id"]!).ToList();
        Assert.Equal(totalWrites, ids?.Distinct().Count());
    }

    // ── 6. Dispose drains channel: no data losses ────────────

    [Fact]
    public void Dispose_DrainsChannel_NoDataLoss()
    {
        var dataDir = Path.Combine(_tempDir, "dispose-drain");
        int totalInserted;

        using (var engine = new SproutEngine(new SproutEngineSettings
        {
            DataDirectory = dataDir,
            FlushInterval = Timeout.InfiniteTimeSpan,
            WalSyncInterval = TimeSpan.Zero,
        }))
        {
            SetupDbAndTable(engine);

            // Fire many writes rapidly
            const int count = 50;
            var tasks = new Task[count];
            for (int i = 0; i < count; i++)
            {
                var idx = i;
                tasks[i] = Task.Run(() =>
                    engine.Execute($"upsert users {{name: 'D{idx}', score: {idx}}}", "testdb"));
            }

            Task.WaitAll(tasks);
            totalInserted = count;
        }
        // Engine disposed here - should drain all pending writes

        // Reopen and verify all data survived
        using (var engine = new SproutEngine(new SproutEngineSettings
        {
            DataDirectory = dataDir,
            FlushInterval = Timeout.InfiniteTimeSpan,
            WalSyncInterval = TimeSpan.Zero,
        }))
        {
            var result = engine.Execute("get users", "testdb");
            Assert.Equal(totalInserted, result.Data?.Count);
        }
    }

    // ── 7. Multiple databases concurrent: no interference ────

    [Fact]
    public void MultipleDatabases_NoInterference()
    {
        using var engine = CreateEngine("multi-db");

        const int dbCount = 4;
        const int rowsPerDb = 20;

        // Create databases and tables
        for (int d = 0; d < dbCount; d++)
        {
            engine.Execute("create database", $"db{d}");
            engine.Execute($"create table items (label string 100, value sint)", $"db{d}");
        }

        // Concurrent writes to different databases
        var tasks = Enumerable.Range(0, dbCount).Select(d => Task.Run(() =>
        {
            for (int r = 0; r < rowsPerDb; r++)
            {
                var result = engine.Execute(
                    $"upsert items {{label: 'DB{d}R{r}', value: {d * 1000 + r}}}",
                    $"db{d}");
                Assert.Null(result.Errors);
            }
        })).ToArray();

        Task.WaitAll(tasks);

        // Verify each database has exactly the right count
        for (int d = 0; d < dbCount; d++)
        {
            var result = engine.Execute("get items", $"db{d}");
            Assert.Equal(rowsPerDb, result.Data?.Count);
        }
    }
}

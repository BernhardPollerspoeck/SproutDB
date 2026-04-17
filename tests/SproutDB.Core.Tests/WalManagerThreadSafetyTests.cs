namespace SproutDB.Core.Tests;

/// <summary>
/// WalManager handles are touched from three independent call sites:
///   - Writer thread (Append via Execute)
///   - Background timer (SyncAll / flush cycle)
///   - Scope manager (Evict when a DB idles out)
///
/// These tests stress the concurrency between them. They don't assert
/// correctness of WAL content — that's covered elsewhere — only that
/// WalManager/WalFile don't throw, deadlock, or corrupt their own state
/// under heavy parallel use.
/// </summary>
public class WalManagerThreadSafetyTests : IDisposable
{
    private readonly string _tempDir;

    public WalManagerThreadSafetyTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), $"sproutdb-walmt-{Guid.NewGuid()}");
        Directory.CreateDirectory(_tempDir);
    }

    public void Dispose()
    {
        if (Directory.Exists(_tempDir))
            Directory.Delete(_tempDir, true);
    }

    [Fact]
    public void GetOrOpen_And_Evict_Concurrent_DoesNotThrow()
    {
        var mgr = new WalManager();
        var dbPaths = Enumerable.Range(0, 20)
            .Select(i =>
            {
                var p = Path.Combine(_tempDir, $"db{i}");
                Directory.CreateDirectory(p);
                return p;
            })
            .ToArray();

        var cts = new CancellationTokenSource(TimeSpan.FromSeconds(3));
        var exceptions = new System.Collections.Concurrent.ConcurrentBag<Exception>();

        var openers = Enumerable.Range(0, 4).Select(_ => Task.Run(() =>
        {
            var rng = new Random();
            try
            {
                while (!cts.IsCancellationRequested)
                {
                    var wal = mgr.GetOrOpen(dbPaths[rng.Next(dbPaths.Length)]);
                    wal.Append("upsert t {x: 1}");
                }
            }
            catch (ObjectDisposedException) { /* expected: evicted mid-append */ }
            catch (Exception ex) { exceptions.Add(ex); }
        })).ToArray();

        var evictors = Enumerable.Range(0, 2).Select(_ => Task.Run(() =>
        {
            var rng = new Random();
            try
            {
                while (!cts.IsCancellationRequested)
                    mgr.Evict(dbPaths[rng.Next(dbPaths.Length)]);
            }
            catch (Exception ex) { exceptions.Add(ex); }
        })).ToArray();

        var syncers = Enumerable.Range(0, 2).Select(_ => Task.Run(() =>
        {
            try
            {
                while (!cts.IsCancellationRequested)
                {
                    mgr.SyncAll();
                    var _ = mgr.GetTotalSizeBytes();
                }
            }
            catch (Exception ex) { exceptions.Add(ex); }
        })).ToArray();

        Task.WaitAll(openers.Concat(evictors).Concat(syncers).ToArray());
        mgr.Dispose();

        Assert.Empty(exceptions);
    }

    [Fact]
    public void SyncAll_AfterEvict_IsNoop_NotCrash()
    {
        var mgr = new WalManager();
        var dbPath = Path.Combine(_tempDir, "db0");
        Directory.CreateDirectory(dbPath);

        var wal = mgr.GetOrOpen(dbPath);
        wal.Append("upsert t {x: 1}");

        mgr.Evict(dbPath);

        // SyncAll must not throw even though the only WalFile was disposed
        mgr.SyncAll();
        mgr.Dispose();
    }

    [Fact]
    public void Append_FromManyThreads_OnSingleWal_StaysConsistent()
    {
        var mgr = new WalManager();
        var dbPath = Path.Combine(_tempDir, "db0");
        Directory.CreateDirectory(dbPath);
        var wal = mgr.GetOrOpen(dbPath);

        const int perThread = 500;
        const int threadCount = 8;

        var tasks = Enumerable.Range(0, threadCount).Select(t => Task.Run(() =>
        {
            for (int i = 0; i < perThread; i++)
                wal.Append($"upsert t {{x: {t}_{i}}}");
        })).ToArray();

        Task.WaitAll(tasks);

        var entries = wal.ReadAll();
        Assert.Equal(threadCount * perThread, entries.Count);

        // Sequence numbers must be unique and contiguous
        var seqs = entries.Select(e => e.Sequence).OrderBy(x => x).ToArray();
        for (int i = 0; i < seqs.Length; i++)
            Assert.Equal(i + 1, seqs[i]);

        mgr.Dispose();
    }
}

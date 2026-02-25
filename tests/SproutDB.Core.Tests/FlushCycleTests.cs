namespace SproutDB.Core.Tests;

public class FlushCycleTests : IDisposable
{
    private readonly string _tempDir;

    public FlushCycleTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), $"sproutdb-flush-test-{Guid.NewGuid()}");
        Directory.CreateDirectory(_tempDir);
    }

    public void Dispose()
    {
        if (Directory.Exists(_tempDir))
            Directory.Delete(_tempDir, true);
    }

    [Fact]
    public void Dispose_FlushesAndTruncatesWal()
    {
        var dataDir = Path.Combine(_tempDir, "data");

        using (var engine = new SproutEngine(new SproutEngineSettings
        {
            DataDirectory = dataDir,
            FlushInterval = Timeout.InfiniteTimeSpan, // disable periodic flush
        }))
        {
            engine.Execute("create database", "testdb");
            engine.Execute("create table users (name string 100)", "testdb");
            engine.Execute("upsert users {name: 'Alice'}", "testdb");
        }
        // Dispose triggers final FlushAll: MMFs flushed + WAL truncated

        // WAL should be empty after dispose
        var walPath = Path.Combine(dataDir, "testdb", "_wal");
        Assert.True(File.Exists(walPath));
        Assert.Equal(0, new FileInfo(walPath).Length);

        // Data should still be readable (was flushed to disk)
        using var engine2 = new SproutEngine(new SproutEngineSettings
        {
            DataDirectory = dataDir,
            FlushInterval = Timeout.InfiniteTimeSpan,
        });
        var r = engine2.Execute("get users", "testdb");
        Assert.Equal(1, r.Data?.Count);
        Assert.Equal("Alice", r.Data?[0]["name"]);
    }

    [Fact]
    public void FlushCycle_TruncatesWalAfterFlush()
    {
        var dataDir = Path.Combine(_tempDir, "data");

        using var engine = new SproutEngine(new SproutEngineSettings
        {
            DataDirectory = dataDir,
            FlushInterval = TimeSpan.FromMilliseconds(100),
        });

        engine.Execute("create database", "testdb");
        engine.Execute("create table users (name string 100)", "testdb");
        engine.Execute("upsert users {name: 'Alice'}", "testdb");

        var walPath = Path.Combine(dataDir, "testdb", "_wal");

        // WAL should have entries right after writes
        Assert.True(new FileInfo(walPath).Length > 0);

        // Wait for flush cycle to run
        Thread.Sleep(300);

        // WAL should be truncated after flush
        Assert.Equal(0, new FileInfo(walPath).Length);

        // Data should still be readable
        var r = engine.Execute("get users", "testdb");
        Assert.Equal(1, r.Data?.Count);
    }

    [Fact]
    public void FlushDisabled_WalNotTruncatedAutomatically()
    {
        var dataDir = Path.Combine(_tempDir, "data");

        using var engine = new SproutEngine(new SproutEngineSettings
        {
            DataDirectory = dataDir,
            FlushInterval = Timeout.InfiniteTimeSpan,
        });

        engine.Execute("create database", "testdb");
        engine.Execute("create table users (name string 100)", "testdb");
        engine.Execute("upsert users {name: 'Alice'}", "testdb");

        Thread.Sleep(200);

        // WAL should still have entries (no automatic flush)
        var walPath = Path.Combine(dataDir, "testdb", "_wal");
        Assert.True(new FileInfo(walPath).Length > 0);
    }

    [Fact]
    public void SettingsConstructor_DefaultFlushInterval()
    {
        var settings = new SproutEngineSettings { DataDirectory = _tempDir };
        Assert.Equal(TimeSpan.FromSeconds(5), settings.FlushInterval);
    }

    [Fact]
    public void StringConstructor_StillWorks()
    {
        var dataDir = Path.Combine(_tempDir, "data");
        using var engine = new SproutEngine(dataDir);

        var r = engine.Execute("create database", "testdb");
        Assert.Equal(SproutOperation.CreateDatabase, r.Operation);
    }

    [Fact]
    public void DataSurvives_AfterFlushCycle_AndRestart()
    {
        var dataDir = Path.Combine(_tempDir, "data");

        using (var engine = new SproutEngine(new SproutEngineSettings
        {
            DataDirectory = dataDir,
            FlushInterval = TimeSpan.FromMilliseconds(100),
        }))
        {
            engine.Execute("create database", "testdb");
            engine.Execute("create table users (name string 100, score sint)", "testdb");
            engine.Execute("upsert users {name: 'Alice', score: 100}", "testdb");
            engine.Execute("upsert users {name: 'Bob', score: 200}", "testdb");

            // Wait for flush cycle
            Thread.Sleep(300);
        }

        // After restart: WAL was truncated by flush, but data is on disk
        using var engine2 = new SproutEngine(new SproutEngineSettings
        {
            DataDirectory = dataDir,
            FlushInterval = Timeout.InfiniteTimeSpan,
        });

        var r = engine2.Execute("get users select name, score", "testdb");
        Assert.Equal(2, r.Data?.Count);
        Assert.Equal("Alice", r.Data?[0]["name"]);
        Assert.Equal(200, r.Data?[1]["score"]);
    }
}

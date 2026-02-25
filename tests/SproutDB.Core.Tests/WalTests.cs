namespace SproutDB.Core.Tests;

public class WalTests : IDisposable
{
    private readonly string _tempDir;

    public WalTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), $"sproutdb-wal-test-{Guid.NewGuid()}");
        Directory.CreateDirectory(_tempDir);
    }

    public void Dispose()
    {
        if (Directory.Exists(_tempDir))
            Directory.Delete(_tempDir, true);
    }

    // ── WAL write + replay: data survives engine restart ─────

    [Fact]
    public void Replay_RecoversInsertedData()
    {
        var dataDir = Path.Combine(_tempDir, "data");

        // First engine: create db + table, insert data
        using (var engine = new SproutEngine(dataDir))
        {
            engine.Execute("create database", "testdb");
            engine.Execute("create table users (name string 100, age ubyte)", "testdb");
            engine.Execute("upsert users {name: 'Alice', age: 28}", "testdb");
            engine.Execute("upsert users {name: 'Bob', age: 35}", "testdb");
        }

        // Second engine: WAL replays, data should be there
        using (var engine = new SproutEngine(dataDir))
        {
            var r = engine.Execute("get users", "testdb");

            Assert.Equal(SproutOperation.Get, r.Operation);
            Assert.Equal(2, r.Data?.Count);
            Assert.Equal("Alice", r.Data?[0]["name"]);
            Assert.Equal("Bob", r.Data?[1]["name"]);
        }
    }

    [Fact]
    public void Replay_PreservesAutoIncrementIds()
    {
        var dataDir = Path.Combine(_tempDir, "data");

        using (var engine = new SproutEngine(dataDir))
        {
            engine.Execute("create database", "testdb");
            engine.Execute("create table users (name string 100)", "testdb");
            engine.Execute("upsert users {name: 'Alice'}", "testdb"); // id=1
            engine.Execute("upsert users {name: 'Bob'}", "testdb");   // id=2
        }

        using (var engine = new SproutEngine(dataDir))
        {
            var r = engine.Execute("get users select id, name", "testdb");

            Assert.Equal((ulong)1, r.Data?[0]["id"]);
            Assert.Equal((ulong)2, r.Data?[1]["id"]);
        }
    }

    [Fact]
    public void Replay_ContinuesAutoIncrementAfterRestart()
    {
        var dataDir = Path.Combine(_tempDir, "data");

        using (var engine = new SproutEngine(dataDir))
        {
            engine.Execute("create database", "testdb");
            engine.Execute("create table users (name string 100)", "testdb");
            engine.Execute("upsert users {name: 'Alice'}", "testdb"); // id=1
        }

        using (var engine = new SproutEngine(dataDir))
        {
            // After replay, next insert should get id=2
            var r = engine.Execute("upsert users {name: 'Bob'}", "testdb");
            Assert.Equal((ulong)2, r.Data?[0]["id"]);
        }
    }

    [Fact]
    public void Replay_HandlesUpdatesCorrectly()
    {
        var dataDir = Path.Combine(_tempDir, "data");

        using (var engine = new SproutEngine(dataDir))
        {
            engine.Execute("create database", "testdb");
            engine.Execute("create table users (name string 100, score sint)", "testdb");
            engine.Execute("upsert users {name: 'Alice', score: 100}", "testdb");
            engine.Execute("upsert users {id: 1, score: 200}", "testdb"); // update
        }

        using (var engine = new SproutEngine(dataDir))
        {
            var r = engine.Execute("get users select name, score", "testdb");

            Assert.Single(r.Data ?? []);
            Assert.Equal("Alice", r.Data?[0]["name"]);
            Assert.Equal(200, r.Data?[0]["score"]);
        }
    }

    [Fact]
    public void Replay_HandlesAddColumn()
    {
        var dataDir = Path.Combine(_tempDir, "data");

        using (var engine = new SproutEngine(dataDir))
        {
            engine.Execute("create database", "testdb");
            engine.Execute("create table users (name string 100)", "testdb");
            engine.Execute("upsert users {name: 'Alice'}", "testdb");
            engine.Execute("add column users.score sint", "testdb");
            engine.Execute("upsert users {id: 1, score: 42}", "testdb");
        }

        using (var engine = new SproutEngine(dataDir))
        {
            var r = engine.Execute("get users select name, score", "testdb");

            Assert.Equal("Alice", r.Data?[0]["name"]);
            Assert.Equal(42, r.Data?[0]["score"]);
        }
    }

    [Fact]
    public void Replay_CreateTableIsIdempotent()
    {
        var dataDir = Path.Combine(_tempDir, "data");

        using (var engine = new SproutEngine(dataDir))
        {
            engine.Execute("create database", "testdb");
            engine.Execute("create table users (name string 100)", "testdb");
        }

        // Replay should not crash even though table already exists on disk
        using (var engine = new SproutEngine(dataDir))
        {
            var r = engine.Execute("get users", "testdb");
            Assert.Equal(SproutOperation.Get, r.Operation);
        }
    }

    [Fact]
    public void Replay_WalIsTruncatedAfterReplay()
    {
        var dataDir = Path.Combine(_tempDir, "data");

        using (var engine = new SproutEngine(dataDir))
        {
            engine.Execute("create database", "testdb");
            engine.Execute("create table users (name string 100)", "testdb");
            engine.Execute("upsert users {name: 'Alice'}", "testdb");
        }

        // After replay + truncation, WAL file should be empty
        using (var engine = new SproutEngine(dataDir))
        {
            // Trigger replay by accessing the database
            engine.Execute("get users", "testdb");
        }

        // Check WAL file is empty
        var walPath = Path.Combine(dataDir, "testdb", "_wal");
        Assert.True(File.Exists(walPath));
        Assert.Equal(0, new FileInfo(walPath).Length);
    }

    [Fact]
    public void Replay_EmptyWal_NoErrors()
    {
        var dataDir = Path.Combine(_tempDir, "data");

        using (var engine = new SproutEngine(dataDir))
        {
            engine.Execute("create database", "testdb");
            engine.Execute("create table users (name string 100)", "testdb");
        }

        // First restart: replays and truncates
        using (var engine = new SproutEngine(dataDir))
        {
            engine.Execute("get users", "testdb");
        }

        // Second restart: WAL is empty, should work fine
        using (var engine = new SproutEngine(dataDir))
        {
            var r = engine.Execute("get users", "testdb");
            Assert.Equal(SproutOperation.Get, r.Operation);
            Assert.Empty(r.Data ?? []);
        }
    }

    [Fact]
    public void Replay_NullValuesPreserved()
    {
        var dataDir = Path.Combine(_tempDir, "data");

        using (var engine = new SproutEngine(dataDir))
        {
            engine.Execute("create database", "testdb");
            engine.Execute("create table users (name string 100, age ubyte)", "testdb");
            engine.Execute("upsert users {name: 'Alice'}", "testdb"); // age=null
        }

        using (var engine = new SproutEngine(dataDir))
        {
            var r = engine.Execute("get users select name, age", "testdb");

            Assert.Equal("Alice", r.Data?[0]["name"]);
            Assert.Null(r.Data?[0]["age"]);
        }
    }

    [Fact]
    public void Replay_DefaultValuesPreserved()
    {
        var dataDir = Path.Combine(_tempDir, "data");

        using (var engine = new SproutEngine(dataDir))
        {
            engine.Execute("create database", "testdb");
            engine.Execute("create table users (name string 100, active bool default true)", "testdb");
            engine.Execute("upsert users {name: 'Alice'}", "testdb"); // active=true (default)
        }

        using (var engine = new SproutEngine(dataDir))
        {
            var r = engine.Execute("get users select name, active", "testdb");

            Assert.Equal("Alice", r.Data?[0]["name"]);
            Assert.Equal(true, r.Data?[0]["active"]);
        }
    }

    [Fact]
    public void Replay_MultipleTablesInSameDatabase()
    {
        var dataDir = Path.Combine(_tempDir, "data");

        using (var engine = new SproutEngine(dataDir))
        {
            engine.Execute("create database", "testdb");
            engine.Execute("create table users (name string 100)", "testdb");
            engine.Execute("create table orders (total sint)", "testdb");
            engine.Execute("upsert users {name: 'Alice'}", "testdb");
            engine.Execute("upsert orders {total: 500}", "testdb");
        }

        using (var engine = new SproutEngine(dataDir))
        {
            var users = engine.Execute("get users", "testdb");
            var orders = engine.Execute("get orders", "testdb");

            Assert.Equal("Alice", users.Data?[0]["name"]);
            Assert.Equal(500, orders.Data?[0]["total"]);
        }
    }

    [Fact]
    public void Replay_ExplicitIdUpsertIsIdempotent()
    {
        var dataDir = Path.Combine(_tempDir, "data");

        using (var engine = new SproutEngine(dataDir))
        {
            engine.Execute("create database", "testdb");
            engine.Execute("create table users (name string 100)", "testdb");
            engine.Execute("upsert users {id: 42, name: 'Alice'}", "testdb");
        }

        using (var engine = new SproutEngine(dataDir))
        {
            var r = engine.Execute("get users select id, name", "testdb");

            Assert.Single(r.Data ?? []);
            Assert.Equal((ulong)42, r.Data?[0]["id"]);
            Assert.Equal("Alice", r.Data?[0]["name"]);
        }
    }
}

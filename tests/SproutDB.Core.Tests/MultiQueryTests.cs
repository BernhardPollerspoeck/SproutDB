namespace SproutDB.Core.Tests;

public class MultiQueryTests : IDisposable
{
    private readonly string _tempDir;
    private readonly SproutEngine _engine;

    public MultiQueryTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), $"sproutdb-test-{Guid.NewGuid()}");
        _engine = new SproutEngine(_tempDir);
        _engine.ExecuteOne("create database", "testdb");
        _engine.ExecuteOne("create table users (name string 100, age ubyte)", "testdb");
        _engine.ExecuteOne("create table orders (product string 100, total sint)", "testdb");
    }

    public void Dispose()
    {
        _engine.Dispose();
        if (Directory.Exists(_tempDir))
            Directory.Delete(_tempDir, true);
    }

    // ── Multi-Query ─────────────────────────────────────────

    [Fact]
    public void MultiQuery_TwoGets_ReturnsTwoResponses()
    {
        _engine.ExecuteOne("upsert users {name: 'Alice', age: 30}", "testdb");
        _engine.ExecuteOne("upsert orders {product: 'Widget', total: 100}", "testdb");

        var results = _engine.Execute("get users; get orders", "testdb");

        Assert.Equal(2, results.Count);
        Assert.Equal(SproutOperation.Get, results[0].Operation);
        Assert.Equal(SproutOperation.Get, results[1].Operation);
        Assert.Equal(1, results[0].Data?.Count);
        Assert.Equal(1, results[1].Data?.Count);
    }

    [Fact]
    public void MultiQuery_EmptySegments_AreSkipped()
    {
        _engine.ExecuteOne("upsert users {name: 'Alice', age: 30}", "testdb");
        _engine.ExecuteOne("upsert orders {product: 'Widget', total: 100}", "testdb");

        var results = _engine.Execute("get users;;get orders", "testdb");

        Assert.Equal(2, results.Count);
        Assert.Equal(SproutOperation.Get, results[0].Operation);
        Assert.Equal(SproutOperation.Get, results[1].Operation);
    }

    [Fact]
    public void SingleQuery_WithoutSemicolon_ReturnsSingleResponse()
    {
        _engine.ExecuteOne("upsert users {name: 'Alice', age: 30}", "testdb");

        var results = _engine.Execute("get users", "testdb");

        Assert.Single(results);
        Assert.Equal(SproutOperation.Get, results[0].Operation);
    }

    [Fact]
    public void MultiQuery_MixedReadWrite_Works()
    {
        var results = _engine.Execute(
            "upsert users {name: 'Alice', age: 30}; get users",
            "testdb");

        Assert.Equal(2, results.Count);
        Assert.Equal(SproutOperation.Upsert, results[0].Operation);
        Assert.Equal(SproutOperation.Get, results[1].Operation);
        Assert.Equal(1, results[1].Data?.Count);
    }

    [Fact]
    public void MultiQuery_ErrorInSecond_FirstStillExecuted()
    {
        var results = _engine.Execute(
            "upsert users {name: 'Alice', age: 30}; get nonexistent",
            "testdb");

        Assert.Equal(2, results.Count);
        Assert.Equal(SproutOperation.Upsert, results[0].Operation);
        Assert.Null(results[0].Errors);
        Assert.Equal(SproutOperation.Error, results[1].Operation);
        Assert.NotNull(results[1].Errors);

        // First upsert should have persisted
        var check = _engine.ExecuteOne("get users", "testdb");
        Assert.Equal(1, check.Data?.Count);
    }

    // ── Transactions ────────────────────────────────────────

    [Fact]
    public void Transaction_Success_BothTablesWritten()
    {
        var results = _engine.Execute(
            "atomic; upsert users {name: 'Alice', age: 30}; upsert orders {product: 'Widget', total: 100}; commit",
            "testdb");

        // 3 responses: Upsert, Upsert, Transaction marker
        Assert.Equal(3, results.Count);
        Assert.Equal(SproutOperation.Upsert, results[0].Operation);
        Assert.Equal(SproutOperation.Upsert, results[1].Operation);
        Assert.Equal(SproutOperation.Transaction, results[2].Operation);
        Assert.Equal(2, results[2].Affected);

        var users = _engine.ExecuteOne("get users", "testdb");
        Assert.Equal(1, users.Data?.Count);

        var orders = _engine.ExecuteOne("get orders", "testdb");
        Assert.Equal(1, orders.Data?.Count);
    }

    [Fact]
    public void Transaction_Rollback_NothingWritten()
    {
        // First upsert succeeds, second hits unknown table → rollback
        var results = _engine.Execute(
            "atomic; upsert users {name: 'Alice', age: 30}; upsert nonexistent {x: 1}; commit",
            "testdb");

        Assert.Single(results);
        Assert.Equal(SproutOperation.Error, results[0].Operation);
        Assert.Contains("transaction rolled back", results[0].Errors?[0].Message ?? "");

        // users should be empty — rolled back
        var users = _engine.ExecuteOne("get users", "testdb");
        Assert.Equal(0, users.Data?.Count ?? 0);
    }

    [Fact]
    public void Transaction_WithoutCommit_ParserError()
    {
        var results = _engine.Execute(
            "atomic; upsert users {name: 'Alice', age: 30}",
            "testdb");

        Assert.Single(results);
        Assert.Equal(SproutOperation.Error, results[0].Operation);
        Assert.Contains("without 'commit'", results[0].Errors?[0].Message ?? "");
    }

    [Fact]
    public void Transaction_NestedAtomic_ParserError()
    {
        var results = _engine.Execute(
            "atomic; atomic; upsert users {name: 'Alice', age: 30}; commit; commit",
            "testdb");

        Assert.NotEmpty(results);
        Assert.Equal(SproutOperation.Error, results[0].Operation);
        Assert.Contains("nested", results[0].Errors?[0].Message ?? "");
    }

    [Fact]
    public void Transaction_GetInsideTransaction_ReturnsData()
    {
        _engine.ExecuteOne("upsert users {name: 'Alice', age: 30}", "testdb");

        var results = _engine.Execute(
            "atomic; upsert users {name: 'Bob', age: 25}; get users; commit",
            "testdb");

        // 3 responses: Upsert, Get, Transaction marker
        Assert.Equal(3, results.Count);
        Assert.Equal(SproutOperation.Upsert, results[0].Operation);
        Assert.Equal(SproutOperation.Get, results[1].Operation);
        // GET sees both Alice (before tx) and Bob (written in tx)
        Assert.Equal(2, results[1].Data?.Count);
        Assert.Equal(SproutOperation.Transaction, results[2].Operation);
    }

    [Fact]
    public void Transaction_MixedWithRegularQuery()
    {
        // Regular query before transaction, and transaction after
        var results = _engine.Execute(
            "upsert users {name: 'Pre', age: 1}; atomic; upsert users {name: 'Alice', age: 30}; upsert orders {product: 'Widget', total: 100}; commit",
            "testdb");

        // 4 responses: Pre-Upsert, Upsert, Upsert, Transaction marker
        Assert.Equal(4, results.Count);
        Assert.Equal(SproutOperation.Upsert, results[0].Operation);
        Assert.Equal(SproutOperation.Upsert, results[1].Operation);
        Assert.Equal(SproutOperation.Upsert, results[2].Operation);
        Assert.Equal(SproutOperation.Transaction, results[3].Operation);
        Assert.Equal(2, results[3].Affected);
    }

    // ── ISproutDatabase.Query ───────────────────────────────

    [Fact]
    public void ISproutDatabase_Query_ReturnsMultipleResponses()
    {
        var db = _engine.GetOrCreateDatabase("testdb");
        db.Query("upsert users {name: 'Alice', age: 30}");

        var results = db.Query("get users; describe users");

        Assert.Equal(2, results.Count);
        Assert.Equal(SproutOperation.Get, results[0].Operation);
        Assert.Equal(SproutOperation.Describe, results[1].Operation);
    }
}

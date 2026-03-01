namespace SproutDB.Core.Tests;

public class IndexTests : IDisposable
{
    private readonly string _tempDir;
    private SproutEngine _engine;
    private bool _disposed;

    public IndexTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), $"sproutdb-test-{Guid.NewGuid()}");
        _engine = new SproutEngine(_tempDir);
        _engine.Execute("create database", "testdb");
        _engine.Execute(
            "create table users (name string 100, email string 200, age ubyte)",
            "testdb");
    }

    public void Dispose()
    {
        if (!_disposed)
        {
            _disposed = true;
            _engine.Dispose();
        }
        if (Directory.Exists(_tempDir))
            Directory.Delete(_tempDir, true);
    }

    // ── Create / Purge Index ──────────────────────────────

    [Fact]
    public void CreateIndex_BuildsCorrectly()
    {
        SeedUsers(5);

        var result = _engine.Execute("create index users.email", "testdb");

        Assert.Equal(SproutOperation.CreateIndex, result.Operation);
        Assert.Equal(1, result.Affected);

        // .btree file should exist
        var btreePath = Path.Combine(_tempDir, "testdb", "users", "email.btree");
        Assert.True(File.Exists(btreePath));
    }

    [Fact]
    public void CreateIndex_AlreadyExists_Error()
    {
        SeedUsers(1);
        _engine.Execute("create index users.email", "testdb");

        var result = _engine.Execute("create index users.email", "testdb");

        Assert.NotNull(result.Errors);
        Assert.Contains("already exists", result.Errors[0].Message);
    }

    [Fact]
    public void CreateIndex_UnknownColumn_Error()
    {
        var result = _engine.Execute("create index users.nonexistent", "testdb");

        Assert.NotNull(result.Errors);
        Assert.Contains("does not exist", result.Errors[0].Message);
    }

    [Fact]
    public void PurgeIndex_RemovesFile()
    {
        SeedUsers(3);
        _engine.Execute("create index users.email", "testdb");

        var result = _engine.Execute("purge index users.email", "testdb");

        Assert.Equal(SproutOperation.PurgeIndex, result.Operation);
        Assert.Equal(1, result.Affected);

        var btreePath = Path.Combine(_tempDir, "testdb", "users", "email.btree");
        Assert.False(File.Exists(btreePath));
    }

    [Fact]
    public void PurgeIndex_NotFound_Error()
    {
        var result = _engine.Execute("purge index users.email", "testdb");

        Assert.NotNull(result.Errors);
        Assert.Contains("does not exist", result.Errors[0].Message);
    }

    // ── GET with Index ─────────────────────────────────────

    [Fact]
    public void GetWithIndex_EqualityLookup_CorrectResult()
    {
        for (int i = 0; i < 100; i++)
            _engine.Execute($"upsert users {{name: 'User{i}', email: 'user{i}@test.com', age: {(i % 50) + 18}}}", "testdb");

        _engine.Execute("create index users.email", "testdb");

        var result = _engine.Execute("get users where email = 'user42@test.com'", "testdb");

        Assert.Null(result.Errors);
        Assert.NotNull(result.Data);
        Assert.Single(result.Data);
        Assert.Equal("User42", result.Data[0]["name"]);
    }

    [Fact]
    public void GetWithIndex_RangeLookup_CorrectResult()
    {
        for (int i = 0; i < 20; i++)
            _engine.Execute($"upsert users {{name: 'User{i}', email: 'user{i}@test.com', age: {i + 18}}}", "testdb");

        _engine.Execute("create index users.age", "testdb");

        // age > 30 means age 31..37 (i=13..19)
        var result = _engine.Execute("get users where age > 30", "testdb");

        Assert.Null(result.Errors);
        Assert.NotNull(result.Data);
        Assert.Equal(7, result.Data.Count);
        foreach (var row in result.Data)
        {
            var age = Convert.ToInt32(row["age"]);
            Assert.True(age > 30);
        }
    }

    [Fact]
    public void GetWithIndex_BetweenLookup_CorrectResult()
    {
        for (int i = 0; i < 20; i++)
            _engine.Execute($"upsert users {{name: 'User{i}', email: 'user{i}@test.com', age: {i + 18}}}", "testdb");

        _engine.Execute("create index users.age", "testdb");

        // age between 25 and 30 → i=7..12 → 6 rows
        var result = _engine.Execute("get users where age between 25 and 30", "testdb");

        Assert.Null(result.Errors);
        Assert.NotNull(result.Data);
        Assert.Equal(6, result.Data.Count);
        foreach (var row in result.Data)
        {
            var age = Convert.ToInt32(row["age"]);
            Assert.True(age >= 25 && age <= 30);
        }
    }

    // ── Upsert updates index ──────────────────────────────

    [Fact]
    public void UpsertUpdatesIndex()
    {
        _engine.Execute("upsert users {name: 'Alice', email: 'alice@test.com', age: 28}", "testdb");
        _engine.Execute("create index users.email", "testdb");

        // Update email
        _engine.Execute("upsert users {id: 1, email: 'newalice@test.com'}", "testdb");

        // Old email should not be found
        var oldResult = _engine.Execute("get users where email = 'alice@test.com'", "testdb");
        Assert.NotNull(oldResult.Data);
        Assert.Empty(oldResult.Data);

        // New email should be found
        var newResult = _engine.Execute("get users where email = 'newalice@test.com'", "testdb");
        Assert.NotNull(newResult.Data);
        Assert.Single(newResult.Data);
        Assert.Equal("Alice", newResult.Data[0]["name"]);
    }

    // ── Delete updates index ──────────────────────────────

    [Fact]
    public void DeleteUpdatesIndex()
    {
        _engine.Execute("upsert users {name: 'Alice', email: 'alice@test.com', age: 28}", "testdb");
        _engine.Execute("upsert users {name: 'Bob', email: 'bob@test.com', age: 35}", "testdb");
        _engine.Execute("create index users.email", "testdb");

        _engine.Execute("delete users where name = 'Alice'", "testdb");

        // Deleted email should not be found
        var result = _engine.Execute("get users where email = 'alice@test.com'", "testdb");
        Assert.NotNull(result.Data);
        Assert.Empty(result.Data);

        // Bob's email should still be found
        var bobResult = _engine.Execute("get users where email = 'bob@test.com'", "testdb");
        Assert.NotNull(bobResult.Data);
        Assert.Single(bobResult.Data);
    }

    // ── B-Tree persist and reload ─────────────────────────

    [Fact]
    public void BTreePersistAndReload()
    {
        SeedUsers(10);
        _engine.Execute("create index users.email", "testdb");

        // Verify lookup works before dispose
        var before = _engine.Execute("get users where email = 'user3@test.com'", "testdb");
        Assert.NotNull(before.Data);
        Assert.Single(before.Data);

        // Dispose and reopen
        _engine.Dispose();
        _disposed = true;

        // Reopen engine — B-Tree should be reloaded from disk
        _engine = new SproutEngine(_tempDir);
        _disposed = false;

        var after = _engine.Execute("get users where email = 'user3@test.com'", "testdb");
        Assert.NotNull(after.Data);
        Assert.Single(after.Data);
        Assert.Equal("User3", after.Data[0]["name"]);
    }

    // ── GetWithoutIndex still works ────────────────────────

    [Fact]
    public void GetWithoutIndex_StillScansCorrectly()
    {
        SeedUsers(5);

        // No index — should still work via full scan
        var result = _engine.Execute("get users where email = 'user2@test.com'", "testdb");

        Assert.Null(result.Errors);
        Assert.NotNull(result.Data);
        Assert.Single(result.Data);
        Assert.Equal("User2", result.Data[0]["name"]);
    }

    // ── Helpers ───────────────────────────────────────────

    private void SeedUsers(int count)
    {
        for (int i = 0; i < count; i++)
            _engine.Execute($"upsert users {{name: 'User{i}', email: 'user{i}@test.com', age: {(i % 50) + 18}}}", "testdb");
    }
}

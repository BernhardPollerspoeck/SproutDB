namespace SproutDB.Core.Tests;

public class PurgeTests : IDisposable
{
    private readonly string _tempDir;
    private readonly SproutEngine _engine;

    public PurgeTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), $"sproutdb-test-{Guid.NewGuid()}");
        _engine = new SproutEngine(_tempDir);
        _engine.Execute("create database", "testdb");
        _engine.Execute("create table users (name string 100, age ubyte)", "testdb");
    }

    public void Dispose()
    {
        _engine.Dispose();
        if (Directory.Exists(_tempDir))
            Directory.Delete(_tempDir, true);
    }

    // ── Purge Column ─────────────────────────────────────────

    [Fact]
    public void PurgeColumn_Success()
    {
        var r = _engine.Execute("purge column users.age", "testdb");

        Assert.Equal(SproutOperation.PurgeColumn, r.Operation);
        Assert.NotNull(r.Schema);
        Assert.Equal("users", r.Schema.Table);
        Assert.Null(r.Errors);
    }

    [Fact]
    public void PurgeColumn_ResponseShowsRemainingColumns()
    {
        var r = _engine.Execute("purge column users.age", "testdb");

        var cols = r.Schema!.Columns!;
        Assert.Equal(2, cols.Count); // id + name (age removed)
        Assert.Equal("id", cols[0].Name);
        Assert.Equal("name", cols[1].Name);
    }

    [Fact]
    public void PurgeColumn_DeletesColFile()
    {
        _engine.Execute("purge column users.age", "testdb");

        var colPath = Path.Combine(_tempDir, "testdb", "users", "age.col");
        Assert.False(File.Exists(colPath));
    }

    [Fact]
    public void PurgeColumn_Idempotent()
    {
        _engine.Execute("purge column users.age", "testdb");
        var r = _engine.Execute("purge column users.age", "testdb");

        Assert.Equal(SproutOperation.PurgeColumn, r.Operation);
        Assert.Null(r.Errors);
    }

    [Fact]
    public void PurgeColumn_UnknownTable_Error()
    {
        var r = _engine.Execute("purge column missing.col", "testdb");

        Assert.Equal(SproutOperation.Error, r.Operation);
        Assert.Equal("UNKNOWN_TABLE", r.Errors![0].Code);
    }

    [Fact]
    public void PurgeColumn_UnknownDatabase_Error()
    {
        var r = _engine.Execute("purge column users.age", "nope");

        Assert.Equal(SproutOperation.Error, r.Operation);
        Assert.Equal("UNKNOWN_DATABASE", r.Errors![0].Code);
    }

    [Fact]
    public void PurgeColumn_DataStillAccessibleAfterPurge()
    {
        // Insert a row, purge a column, then get — remaining data should work
        _engine.Execute("upsert users {name: 'John', age: 25}", "testdb");
        _engine.Execute("purge column users.age", "testdb");

        var r = _engine.Execute("get users", "testdb");

        Assert.Equal(SproutOperation.Get, r.Operation);
        Assert.Single(r.Data!);
        Assert.Equal("John", r.Data[0]["name"]);
        Assert.False(r.Data[0].ContainsKey("age"));
    }

    // ── Purge Table ──────────────────────────────────────────

    [Fact]
    public void PurgeTable_Success()
    {
        var r = _engine.Execute("purge table users", "testdb");

        Assert.Equal(SproutOperation.PurgeTable, r.Operation);
        Assert.NotNull(r.Schema);
        Assert.Equal("users", r.Schema.Table);
        Assert.Null(r.Errors);
    }

    [Fact]
    public void PurgeTable_DeletesDirectory()
    {
        _engine.Execute("purge table users", "testdb");

        var tablePath = Path.Combine(_tempDir, "testdb", "users");
        Assert.False(Directory.Exists(tablePath));
    }

    [Fact]
    public void PurgeTable_Idempotent()
    {
        _engine.Execute("purge table users", "testdb");
        var r = _engine.Execute("purge table users", "testdb");

        Assert.Equal(SproutOperation.PurgeTable, r.Operation);
        Assert.Null(r.Errors);
    }

    [Fact]
    public void PurgeTable_CanRecreateAfterPurge()
    {
        _engine.Execute("purge table users", "testdb");
        var r = _engine.Execute("create table users (email string 320)", "testdb");

        Assert.Equal(SproutOperation.CreateTable, r.Operation);
        Assert.Null(r.Errors);
    }

    // ── Purge Database ───────────────────────────────────────

    [Fact]
    public void PurgeDatabase_Success()
    {
        var r = _engine.Execute("purge database", "testdb");

        Assert.Equal(SproutOperation.PurgeDatabase, r.Operation);
        Assert.NotNull(r.Schema);
        Assert.Equal("testdb", r.Schema.Database);
        Assert.Null(r.Errors);
    }

    [Fact]
    public void PurgeDatabase_DeletesDirectory()
    {
        _engine.Execute("purge database", "testdb");

        var dbPath = Path.Combine(_tempDir, "testdb");
        Assert.False(Directory.Exists(dbPath));
    }

    [Fact]
    public void PurgeDatabase_Idempotent()
    {
        _engine.Execute("purge database", "testdb");
        var r = _engine.Execute("purge database", "testdb");

        Assert.Equal(SproutOperation.PurgeDatabase, r.Operation);
        Assert.Null(r.Errors);
    }

    [Fact]
    public void PurgeDatabase_CanRecreateAfterPurge()
    {
        _engine.Execute("purge database", "testdb");
        var r = _engine.Execute("create database", "testdb");

        Assert.Equal(SproutOperation.CreateDatabase, r.Operation);
        Assert.Null(r.Errors);
    }

    [Fact]
    public void PurgeDatabase_WithMultipleTables()
    {
        _engine.Execute("create table orders (total uint)", "testdb");
        _engine.Execute("upsert users {name: 'John'}", "testdb");
        _engine.Execute("upsert orders {total: 100}", "testdb");

        var r = _engine.Execute("purge database", "testdb");

        Assert.Equal(SproutOperation.PurgeDatabase, r.Operation);
        Assert.Null(r.Errors);
        Assert.False(Directory.Exists(Path.Combine(_tempDir, "testdb")));
    }
}

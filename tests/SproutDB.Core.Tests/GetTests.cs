namespace SproutDB.Core.Tests;

public class GetTests : IDisposable
{
    private readonly string _tempDir;
    private readonly SproutEngine _engine;

    public GetTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), $"sproutdb-test-{Guid.NewGuid()}");
        _engine = new SproutEngine(_tempDir);
        _engine.Execute("create database", "testdb");
        _engine.Execute(
            "create table users (name string 100, email string 320, age ubyte, active bool default true)",
            "testdb");

        // Seed data
        _engine.Execute("upsert users {name: 'Alice', email: 'alice@test.com', age: 28}", "testdb");
        _engine.Execute("upsert users {name: 'Bob', email: 'bob@test.com', age: 35}", "testdb");
        _engine.Execute("upsert users {name: 'Charlie', email: 'charlie@test.com', age: 22}", "testdb");
    }

    public void Dispose()
    {
        _engine.Dispose();
        if (Directory.Exists(_tempDir))
            Directory.Delete(_tempDir, true);
    }

    // ── Get all ───────────────────────────────────────────────

    [Fact]
    public void GetAll_ReturnsAllRows()
    {
        var r = _engine.Execute("get users", "testdb");

        Assert.Equal(SproutOperation.Get, r.Operation);
        Assert.Equal(3, r.Affected);
        Assert.NotNull(r.Data);
        Assert.Equal(3, r.Data.Count);
    }

    [Fact]
    public void GetAll_ReturnsAllColumns()
    {
        var r = _engine.Execute("get users", "testdb");

        var row = r.Data![0];
        Assert.True(row.ContainsKey("id"));
        Assert.True(row.ContainsKey("name"));
        Assert.True(row.ContainsKey("email"));
        Assert.True(row.ContainsKey("age"));
        Assert.True(row.ContainsKey("active"));
    }

    [Fact]
    public void GetAll_RowsOrderedById()
    {
        var r = _engine.Execute("get users", "testdb");

        Assert.Equal((ulong)1, r.Data![0]["id"]);
        Assert.Equal((ulong)2, r.Data[1]["id"]);
        Assert.Equal((ulong)3, r.Data[2]["id"]);
    }

    [Fact]
    public void GetAll_CorrectValues()
    {
        var r = _engine.Execute("get users", "testdb");

        var alice = r.Data![0];
        Assert.Equal("Alice", alice["name"]);
        Assert.Equal("alice@test.com", alice["email"]);
        Assert.Equal((byte)28, alice["age"]);
        Assert.Equal(true, alice["active"]);
    }

    [Fact]
    public void GetAll_EmptyTable_ReturnsEmptyData()
    {
        _engine.Execute("create table empty (name string)", "testdb");
        var r = _engine.Execute("get empty", "testdb");

        Assert.Equal(SproutOperation.Get, r.Operation);
        Assert.Equal(0, r.Affected);
        Assert.NotNull(r.Data);
        Assert.Empty(r.Data);
    }

    // ── Select projection ─────────────────────────────────────

    [Fact]
    public void GetSelect_SingleColumn()
    {
        var r = _engine.Execute("get users select name", "testdb");

        Assert.Equal(3, r.Data!.Count);
        var row = r.Data[0];
        Assert.Single(row);
        Assert.Equal("Alice", row["name"]);
    }

    [Fact]
    public void GetSelect_MultipleColumns()
    {
        var r = _engine.Execute("get users select name, age", "testdb");

        var row = r.Data![0];
        Assert.Equal(2, row.Count);
        Assert.Equal("Alice", row["name"]);
        Assert.Equal((byte)28, row["age"]);
    }

    [Fact]
    public void GetSelect_WithId()
    {
        var r = _engine.Execute("get users select id, name", "testdb");

        var row = r.Data![0];
        Assert.Equal(2, row.Count);
        Assert.Equal((ulong)1, row["id"]);
        Assert.Equal("Alice", row["name"]);
    }

    [Fact]
    public void GetSelect_OnlyId()
    {
        var r = _engine.Execute("get users select id", "testdb");

        var row = r.Data![0];
        Assert.Single(row);
        Assert.Equal((ulong)1, row["id"]);
    }

    [Fact]
    public void GetSelect_ExcludesNonSelectedColumns()
    {
        var r = _engine.Execute("get users select name", "testdb");

        var row = r.Data![0];
        Assert.False(row.ContainsKey("id"));
        Assert.False(row.ContainsKey("email"));
        Assert.False(row.ContainsKey("age"));
        Assert.False(row.ContainsKey("active"));
    }

    // ── Null values ───────────────────────────────────────────

    [Fact]
    public void Get_NullValues_ReturnedAsNull()
    {
        _engine.Execute("upsert users {}", "testdb"); // id=4, all nullable columns null
        var r = _engine.Execute("get users select name, age", "testdb");

        var lastRow = r.Data![3]; // id=4
        Assert.Null(lastRow["name"]);
        Assert.Null(lastRow["age"]);
    }

    [Fact]
    public void Get_DefaultValues_Returned()
    {
        _engine.Execute("upsert users {}", "testdb"); // active defaults to true
        var r = _engine.Execute("get users select active", "testdb");

        var lastRow = r.Data![3];
        Assert.Equal(true, lastRow["active"]);
    }

    // ── After updates ─────────────────────────────────────────

    [Fact]
    public void Get_AfterUpdate_ReturnsUpdatedValues()
    {
        _engine.Execute("upsert users {id: 1, name: 'Alice Updated'}", "testdb");
        var r = _engine.Execute("get users select id, name", "testdb");

        Assert.Equal("Alice Updated", r.Data![0]["name"]);
    }

    // ── Error cases ───────────────────────────────────────────

    [Fact]
    public void Get_UnknownTable_Error()
    {
        var r = _engine.Execute("get nonexistent", "testdb");

        Assert.Equal(SproutOperation.Error, r.Operation);
        Assert.Equal("UNKNOWN_TABLE", r.Errors![0].Code);
    }

    [Fact]
    public void Get_UnknownDatabase_Error()
    {
        var r = _engine.Execute("get users", "nodb");

        Assert.Equal(SproutOperation.Error, r.Operation);
        Assert.Equal("UNKNOWN_DATABASE", r.Errors![0].Code);
    }

    [Fact]
    public void Get_UnknownColumn_InSelect_Error()
    {
        var r = _engine.Execute("get users select nonexistent", "testdb");

        Assert.Equal(SproutOperation.Error, r.Operation);
        Assert.Equal("UNKNOWN_COLUMN", r.Errors![0].Code);
        Assert.Contains("nonexistent", r.Errors[0].Message);
    }

    [Fact]
    public void Get_CaseInsensitive()
    {
        var r = _engine.Execute("GET users SELECT name", "testdb");

        Assert.Equal(SproutOperation.Get, r.Operation);
        Assert.Equal(3, r.Data!.Count);
    }

    // ── Response format ───────────────────────────────────────

    [Fact]
    public void Get_NoErrors_NullErrorsAndAnnotatedQuery()
    {
        var r = _engine.Execute("get users", "testdb");

        Assert.Null(r.Errors);
        Assert.Null(r.AnnotatedQuery);
        Assert.Null(r.Schema);
        Assert.Null(r.Paging);
    }
}

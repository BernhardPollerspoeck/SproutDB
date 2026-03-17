namespace SproutDB.Core.Tests;

public class AlterColumnTests : IDisposable
{
    private readonly string _tempDir;
    private readonly SproutEngine _engine;

    public AlterColumnTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), $"sproutdb-test-{Guid.NewGuid()}");
        _engine = new SproutEngine(_tempDir);
        _engine.ExecuteOne("create database", "testdb");
        _engine.ExecuteOne("create table users (name string 100, age ubyte)", "testdb");
    }

    public void Dispose()
    {
        _engine.Dispose();
        if (Directory.Exists(_tempDir))
            Directory.Delete(_tempDir, true);
    }

    // ── Expand ───────────────────────────────────────────────

    [Fact]
    public void AlterColumn_ExpandSize_Success()
    {
        var r = _engine.ExecuteOne("alter column users.name string 500", "testdb");

        Assert.Equal(SproutOperation.AlterColumn, r.Operation);
        Assert.Null(r.Errors);

        var col = r.Schema!.Columns!.First(c => c.Name == "name");
        Assert.Equal(500, col.Size);
    }

    [Fact]
    public void AlterColumn_ExpandSize_DataPreserved()
    {
        _engine.ExecuteOne("upsert users {name: 'Alice', age: 25}", "testdb");
        _engine.ExecuteOne("upsert users {name: 'Bob', age: 30}", "testdb");

        _engine.ExecuteOne("alter column users.name string 500", "testdb");

        var r = _engine.ExecuteOne("get users", "testdb");
        Assert.Equal(2, r.Data!.Count);
        Assert.Equal("Alice", r.Data[0]["name"]);
        Assert.Equal("Bob", r.Data[1]["name"]);
    }

    [Fact]
    public void AlterColumn_ExpandSize_CanWriteLongerStrings()
    {
        _engine.ExecuteOne("alter column users.name string 500", "testdb");

        var longName = new string('x', 400);
        var r = _engine.ExecuteOne($"upsert users {{name: '{longName}'}}", "testdb");

        Assert.Equal(SproutOperation.Upsert, r.Operation);
        Assert.Equal(longName, r.Data![0]["name"]);
    }

    [Fact]
    public void AlterColumn_ExpandSize_FileGrows()
    {
        var colPath = Path.Combine(_tempDir, "testdb", "users", "name.col");
        var sizeBefore = new FileInfo(colPath).Length;

        _engine.ExecuteOne("alter column users.name string 500", "testdb");

        var sizeAfter = new FileInfo(colPath).Length;
        Assert.True(sizeAfter > sizeBefore);
    }

    // ── Shrink ───────────────────────────────────────────────

    [Fact]
    public void AlterColumn_ShrinkSize_Success()
    {
        var r = _engine.ExecuteOne("alter column users.name string 50", "testdb");

        Assert.Equal(SproutOperation.AlterColumn, r.Operation);
        Assert.Null(r.Errors);

        var col = r.Schema!.Columns!.First(c => c.Name == "name");
        Assert.Equal(50, col.Size);
    }

    [Fact]
    public void AlterColumn_ShrinkSize_DataTruncated()
    {
        // Insert a name that's longer than the new size
        _engine.ExecuteOne("upsert users {name: 'Alexander Hamilton'}", "testdb");

        _engine.ExecuteOne("alter column users.name string 5", "testdb");

        var r = _engine.ExecuteOne("get users", "testdb");
        Assert.Equal("Alexa", r.Data![0]["name"]);
    }

    [Fact]
    public void AlterColumn_ShrinkSize_ShortDataPreserved()
    {
        _engine.ExecuteOne("upsert users {name: 'Bob'}", "testdb");

        _engine.ExecuteOne("alter column users.name string 50", "testdb");

        var r = _engine.ExecuteOne("get users", "testdb");
        Assert.Equal("Bob", r.Data![0]["name"]);
    }

    // ── Idempotent ───────────────────────────────────────────

    [Fact]
    public void AlterColumn_SameSize_NoOp()
    {
        var r = _engine.ExecuteOne("alter column users.name string 100", "testdb");

        Assert.Equal(SproutOperation.AlterColumn, r.Operation);
        Assert.Null(r.Errors);
    }

    // ── Error cases ──────────────────────────────────────────

    [Fact]
    public void AlterColumn_UnknownColumn_Error()
    {
        var r = _engine.ExecuteOne("alter column users.missing string 100", "testdb");

        Assert.Equal(SproutOperation.Error, r.Operation);
        Assert.Equal("UNKNOWN_COLUMN", r.Errors![0].Code);
    }

    [Fact]
    public void AlterColumn_NotStringColumn_Error()
    {
        var r = _engine.ExecuteOne("alter column users.age string 100", "testdb");

        Assert.Equal(SproutOperation.Error, r.Operation);
        Assert.Contains("only string columns", r.Errors![0].Message);
    }

    [Fact]
    public void AlterColumn_UnknownTable_Error()
    {
        var r = _engine.ExecuteOne("alter column missing.name string 100", "testdb");

        Assert.Equal(SproutOperation.Error, r.Operation);
        Assert.Equal("UNKNOWN_TABLE", r.Errors![0].Code);
    }

    [Fact]
    public void AlterColumn_UnknownDatabase_Error()
    {
        var r = _engine.ExecuteOne("alter column users.name string 100", "nope");

        Assert.Equal(SproutOperation.Error, r.Operation);
        Assert.Equal("UNKNOWN_DATABASE", r.Errors![0].Code);
    }

    [Fact]
    public void AlterColumn_NullValuesPreserved()
    {
        _engine.ExecuteOne("upsert users {age: 25}", "testdb"); // name is null

        _engine.ExecuteOne("alter column users.name string 500", "testdb");

        var r = _engine.ExecuteOne("get users", "testdb");
        Assert.Null(r.Data![0]["name"]);
    }
}

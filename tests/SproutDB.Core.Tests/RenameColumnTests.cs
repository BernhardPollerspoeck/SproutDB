namespace SproutDB.Core.Tests;

public class RenameColumnTests : IDisposable
{
    private readonly string _tempDir;
    private readonly SproutEngine _engine;

    public RenameColumnTests()
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

    [Fact]
    public void RenameColumn_Success()
    {
        var r = _engine.Execute("rename column users.name to username", "testdb");

        Assert.Equal(SproutOperation.RenameColumn, r.Operation);
        Assert.NotNull(r.Schema);
        Assert.Equal("users", r.Schema.Table);
        Assert.Null(r.Errors);
    }

    [Fact]
    public void RenameColumn_SchemaUpdated()
    {
        var r = _engine.Execute("rename column users.name to username", "testdb");

        var cols = r.Schema!.Columns!;
        Assert.Contains(cols, c => c.Name == "username");
        Assert.DoesNotContain(cols, c => c.Name == "name");
    }

    [Fact]
    public void RenameColumn_FileRenamed()
    {
        _engine.Execute("rename column users.name to username", "testdb");

        var oldPath = Path.Combine(_tempDir, "testdb", "users", "name.col");
        var newPath = Path.Combine(_tempDir, "testdb", "users", "username.col");
        Assert.False(File.Exists(oldPath));
        Assert.True(File.Exists(newPath));
    }

    [Fact]
    public void RenameColumn_DataAccessibleWithNewName()
    {
        _engine.Execute("upsert users {name: 'John', age: 25}", "testdb");
        _engine.Execute("rename column users.name to username", "testdb");

        var r = _engine.Execute("get users", "testdb");

        Assert.Single(r.Data!);
        Assert.Equal("John", r.Data[0]["username"]);
        Assert.False(r.Data[0].ContainsKey("name"));
    }

    [Fact]
    public void RenameColumn_CanUpsertWithNewName()
    {
        _engine.Execute("rename column users.name to username", "testdb");
        var r = _engine.Execute("upsert users {username: 'Jane', age: 30}", "testdb");

        Assert.Equal(SproutOperation.Upsert, r.Operation);
        Assert.Equal("Jane", r.Data![0]["username"]);
    }

    [Fact]
    public void RenameColumn_SameName_NoOp()
    {
        var r = _engine.Execute("rename column users.name to name", "testdb");

        Assert.Equal(SproutOperation.RenameColumn, r.Operation);
        Assert.Null(r.Errors);
    }

    [Fact]
    public void RenameColumn_Idempotent()
    {
        _engine.Execute("rename column users.name to username", "testdb");
        var r = _engine.Execute("rename column users.name to username", "testdb");

        Assert.Equal(SproutOperation.RenameColumn, r.Operation);
        Assert.Null(r.Errors);
    }

    [Fact]
    public void RenameColumn_UnknownColumn_Error()
    {
        var r = _engine.Execute("rename column users.missing to newname", "testdb");

        Assert.Equal(SproutOperation.Error, r.Operation);
        Assert.Equal("UNKNOWN_COLUMN", r.Errors![0].Code);
    }

    [Fact]
    public void RenameColumn_TargetExists_Error()
    {
        var r = _engine.Execute("rename column users.name to age", "testdb");

        Assert.Equal(SproutOperation.Error, r.Operation);
        Assert.Contains("already exists", r.Errors![0].Message);
    }

    [Fact]
    public void RenameColumn_UnknownTable_Error()
    {
        var r = _engine.Execute("rename column missing.col to newcol", "testdb");

        Assert.Equal(SproutOperation.Error, r.Operation);
        Assert.Equal("UNKNOWN_TABLE", r.Errors![0].Code);
    }

    [Fact]
    public void RenameColumn_UnknownDatabase_Error()
    {
        var r = _engine.Execute("rename column users.name to username", "nope");

        Assert.Equal(SproutOperation.Error, r.Operation);
        Assert.Equal("UNKNOWN_DATABASE", r.Errors![0].Code);
    }
}

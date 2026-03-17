namespace SproutDB.Core.Tests;

public class ColumnAliasTests : IDisposable
{
    private readonly string _tempDir;
    private readonly SproutEngine _engine;

    public ColumnAliasTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), $"sproutdb-test-{Guid.NewGuid()}");
        _engine = new SproutEngine(_tempDir);
        _engine.ExecuteOne("create database", "testdb");

        _engine.ExecuteOne(
            "create table users (name string 50, email string 100, age sint)",
            "testdb");

        _engine.ExecuteOne("upsert users {name: 'Alice', email: 'alice@test.de', age: 30}", "testdb");
        _engine.ExecuteOne("upsert users {name: 'Bob', email: 'bob@test.de', age: 25}", "testdb");
    }

    public void Dispose()
    {
        _engine.Dispose();
        if (Directory.Exists(_tempDir))
            Directory.Delete(_tempDir, true);
    }

    [Fact]
    public void SelectAs_SingleColumn_RenamesKey()
    {
        var r = _engine.ExecuteOne("get users select name as username", "testdb");

        Assert.Equal(SproutOperation.Get, r.Operation);
        Assert.NotNull(r.Data);
        Assert.Equal(2, r.Data.Count);

        var first = r.Data[0];
        Assert.True(first.ContainsKey("username"));
        Assert.False(first.ContainsKey("name"));
        Assert.Equal("Alice", first["username"]);
    }

    [Fact]
    public void SelectAs_MultipleColumns_RenamesBoth()
    {
        var r = _engine.ExecuteOne("get users select name as username, email as contact", "testdb");

        Assert.NotNull(r.Data);
        var first = r.Data[0];
        Assert.True(first.ContainsKey("username"));
        Assert.True(first.ContainsKey("contact"));
        Assert.False(first.ContainsKey("name"));
        Assert.False(first.ContainsKey("email"));
    }

    [Fact]
    public void SelectAs_MixedAliasAndPlain()
    {
        var r = _engine.ExecuteOne("get users select name as username, email, age", "testdb");

        Assert.NotNull(r.Data);
        var first = r.Data[0];
        Assert.True(first.ContainsKey("username"));
        Assert.True(first.ContainsKey("email"));
        Assert.True(first.ContainsKey("age"));
        Assert.False(first.ContainsKey("name"));
    }

    [Fact]
    public void SelectAs_IdColumn_CanBeAliased()
    {
        var r = _engine.ExecuteOne("get users select _id as user_id, name", "testdb");

        Assert.NotNull(r.Data);
        var first = r.Data[0];
        Assert.True(first.ContainsKey("user_id"));
        Assert.False(first.ContainsKey("_id"));
        Assert.True(first.ContainsKey("name"));
    }

    [Fact]
    public void SelectAs_WithWhere_FiltersAndRenames()
    {
        var r = _engine.ExecuteOne("get users select name as username where age > 25", "testdb");

        Assert.NotNull(r.Data);
        Assert.Single(r.Data);
        Assert.Equal("Alice", r.Data[0]["username"]);
    }

    [Fact]
    public void SelectAs_WithComputedField_BothWork()
    {
        var r = _engine.ExecuteOne("get users select name as username, age * 2 as double_age", "testdb");

        Assert.NotNull(r.Data);
        var first = r.Data[0];
        Assert.True(first.ContainsKey("username"));
        Assert.True(first.ContainsKey("double_age"));
        Assert.False(first.ContainsKey("name"));
    }

    [Fact]
    public void SelectAs_InFollowSelect_RenamesKey()
    {
        _engine.ExecuteOne("create table orders (user_id ulong, total sint, status string 50)", "testdb");
        _engine.ExecuteOne("upsert orders {user_id: 1, total: 100, status: 'completed'}", "testdb");

        var r = _engine.ExecuteOne(
            "get users follow users._id -> orders.user_id as orders select total as order_total, status as order_status",
            "testdb");

        Assert.Equal(SproutOperation.Get, r.Operation);
        Assert.NotNull(r.Data);
        Assert.Single(r.Data);

        var row = r.Data[0];
        Assert.True(row.ContainsKey("order_total"));
        Assert.True(row.ContainsKey("order_status"));
        Assert.False(row.ContainsKey("orders.total"));
        Assert.False(row.ContainsKey("orders.status"));
        Assert.False(row.ContainsKey("orders.order_total"));
    }

    [Fact]
    public void SelectAs_MissingAliasName_Error()
    {
        var r = _engine.ExecuteOne("get users select name as", "testdb");

        Assert.Equal(SproutOperation.Error, r.Operation);
        Assert.NotNull(r.Errors);
    }
}

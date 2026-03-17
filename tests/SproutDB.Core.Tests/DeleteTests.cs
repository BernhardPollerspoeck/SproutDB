namespace SproutDB.Core.Tests;

public class DeleteTests : IDisposable
{
    private readonly string _tempDir;
    private readonly SproutEngine _engine;

    public DeleteTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), $"sproutdb-test-{Guid.NewGuid()}");
        _engine = new SproutEngine(_tempDir);
        _engine.ExecuteOne("create database", "testdb");
        _engine.ExecuteOne(
            "create table users (name string 100, age ubyte)",
            "testdb");

        // Seed: Alice(28), Bob(35), Charlie(22), Diana(28)
        _engine.ExecuteOne("upsert users {name: 'Alice', age: 28}", "testdb");
        _engine.ExecuteOne("upsert users {name: 'Bob', age: 35}", "testdb");
        _engine.ExecuteOne("upsert users {name: 'Charlie', age: 22}", "testdb");
        _engine.ExecuteOne("upsert users {name: 'Diana', age: 28}", "testdb");
    }

    public void Dispose()
    {
        _engine.Dispose();
        if (Directory.Exists(_tempDir))
            Directory.Delete(_tempDir, true);
    }

    [Fact]
    public void Delete_ByCondition()
    {
        var r = _engine.ExecuteOne("delete users where age < 25", "testdb");

        Assert.Equal(SproutOperation.Delete, r.Operation);
        Assert.Equal(1, r.Affected); // Charlie(22)
        Assert.Null(r.Errors);
    }

    [Fact]
    public void Delete_ById()
    {
        var r = _engine.ExecuteOne("delete users where _id = 1", "testdb");

        Assert.Equal(SproutOperation.Delete, r.Operation);
        Assert.Equal(1, r.Affected);
    }

    [Fact]
    public void Delete_MultipleRows()
    {
        var r = _engine.ExecuteOne("delete users where age = 28", "testdb");

        Assert.Equal(SproutOperation.Delete, r.Operation);
        Assert.Equal(2, r.Affected); // Alice + Diana
    }

    [Fact]
    public void Delete_NoMatch()
    {
        var r = _engine.ExecuteOne("delete users where age > 100", "testdb");

        Assert.Equal(SproutOperation.Delete, r.Operation);
        Assert.Equal(0, r.Affected);
    }

    [Fact]
    public void Delete_WithoutWhere_Error()
    {
        var r = _engine.ExecuteOne("delete users", "testdb");

        Assert.Equal(SproutOperation.Error, r.Operation);
        Assert.NotNull(r.Errors);
        Assert.Contains(r.Errors, e => e.Code == "WHERE_REQUIRED");
    }

    [Fact]
    public void Delete_UnknownColumn_Error()
    {
        var r = _engine.ExecuteOne("delete users where xyz = 1", "testdb");

        Assert.Equal(SproutOperation.Error, r.Operation);
        Assert.NotNull(r.Errors);
        Assert.Contains(r.Errors, e => e.Code == "UNKNOWN_COLUMN");
    }

    [Fact]
    public void Delete_ThenGet_RowsGone()
    {
        _engine.ExecuteOne("delete users where age < 25", "testdb");

        var r = _engine.ExecuteOne("get users", "testdb");

        Assert.Equal(3, r.Affected); // Alice, Bob, Diana remain
        Assert.DoesNotContain(r.Data!, row => (string)row["name"]! == "Charlie");
    }

    [Fact]
    public void Delete_ThenUpsert_ReusesPlace()
    {
        // Delete Alice (id=1)
        _engine.ExecuteOne("delete users where _id = 1", "testdb");

        // Insert new user — should reuse place
        var ins = _engine.ExecuteOne("upsert users {name: 'Eve', age: 30}", "testdb");
        Assert.Equal(SproutOperation.Upsert, ins.Operation);

        // Eve gets a new id (5), but the storage place should be reused
        var r = _engine.ExecuteOne("get users", "testdb");
        Assert.Equal(4, r.Affected); // Bob, Charlie, Diana, Eve
        Assert.Contains(r.Data!, row => (string)row["name"]! == "Eve");
        Assert.DoesNotContain(r.Data!, row => (string)row["name"]! == "Alice");
    }

    [Fact]
    public void Delete_AllRows()
    {
        var r = _engine.ExecuteOne("delete users where _id > 0", "testdb");

        Assert.Equal(4, r.Affected);

        var get = _engine.ExecuteOne("get users", "testdb");
        Assert.Equal(0, get.Affected);
        Assert.Empty(get.Data!);
    }

    [Fact]
    public void Delete_Idempotent()
    {
        var r1 = _engine.ExecuteOne("delete users where age < 25", "testdb");
        Assert.Equal(1, r1.Affected);

        var r2 = _engine.ExecuteOne("delete users where age < 25", "testdb");
        Assert.Equal(0, r2.Affected);
    }

    [Fact]
    public void Delete_ComplexWhere()
    {
        // Delete where age = 28 and name = 'Alice' → only Alice
        var r = _engine.ExecuteOne("delete users where age = 28 and name = 'Alice'", "testdb");

        Assert.Equal(1, r.Affected);

        var get = _engine.ExecuteOne("get users", "testdb");
        Assert.Equal(3, get.Affected); // Bob, Charlie, Diana
        Assert.DoesNotContain(get.Data!, row => (string)row["name"]! == "Alice");
        Assert.Contains(get.Data!, row => (string)row["name"]! == "Diana");
    }
}

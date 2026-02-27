namespace SproutDB.Core.Tests;

public class WhereTests : IDisposable
{
    private readonly string _tempDir;
    private readonly SproutEngine _engine;

    public WhereTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), $"sproutdb-test-{Guid.NewGuid()}");
        _engine = new SproutEngine(_tempDir);
        _engine.Execute("create database", "testdb");
        _engine.Execute(
            "create table users (name string 100, age ubyte, score sint, rating double)",
            "testdb");

        // Seed: Alice(28, 85, 4.5), Bob(35, 92, 3.8), Charlie(22, 70, 4.9), Diana(28, 88, 4.5)
        _engine.Execute("upsert users {name: 'Alice', age: 28, score: 85, rating: 4.5}", "testdb");
        _engine.Execute("upsert users {name: 'Bob', age: 35, score: 92, rating: 3.8}", "testdb");
        _engine.Execute("upsert users {name: 'Charlie', age: 22, score: 70, rating: 4.9}", "testdb");
        _engine.Execute("upsert users {name: 'Diana', age: 28, score: 88, rating: 4.5}", "testdb");
    }

    public void Dispose()
    {
        _engine.Dispose();
        if (Directory.Exists(_tempDir))
            Directory.Delete(_tempDir, true);
    }

    // ── Equal (=) ─────────────────────────────────────────────

    [Fact]
    public void Where_Equal_String()
    {
        var r = _engine.Execute("get users where name = 'Alice'", "testdb");

        Assert.Equal(SproutOperation.Get, r.Operation);
        Assert.Equal(1, r.Affected);
        Assert.Equal("Alice", r.Data![0]["name"]);
    }

    [Fact]
    public void Where_Equal_Integer()
    {
        var r = _engine.Execute("get users where age = 28", "testdb");

        Assert.Equal(2, r.Affected); // Alice + Diana
        Assert.All(r.Data!, row => Assert.Equal((byte)28, row["age"]));
    }

    [Fact]
    public void Where_Equal_NoMatch()
    {
        var r = _engine.Execute("get users where age = 99", "testdb");

        Assert.Equal(0, r.Affected);
        Assert.Empty(r.Data!);
    }

    // ── Not Equal (!=) ────────────────────────────────────────

    [Fact]
    public void Where_NotEqual()
    {
        var r = _engine.Execute("get users where age != 28", "testdb");

        Assert.Equal(2, r.Affected); // Bob + Charlie
        Assert.All(r.Data!, row => Assert.NotEqual((byte)28, row["age"]));
    }

    // ── Greater Than (>) ──────────────────────────────────────

    [Fact]
    public void Where_GreaterThan()
    {
        var r = _engine.Execute("get users where age > 28", "testdb");

        Assert.Equal(1, r.Affected); // Bob(35)
        Assert.Equal("Bob", r.Data![0]["name"]);
    }

    [Fact]
    public void Where_GreaterThan_NoMatch()
    {
        var r = _engine.Execute("get users where age > 100", "testdb");

        Assert.Equal(0, r.Affected);
    }

    // ── Greater Than Or Equal (>=) ────────────────────────────

    [Fact]
    public void Where_GreaterThanOrEqual()
    {
        var r = _engine.Execute("get users where age >= 28", "testdb");

        Assert.Equal(3, r.Affected); // Alice, Bob, Diana
    }

    // ── Less Than (<) ─────────────────────────────────────────

    [Fact]
    public void Where_LessThan()
    {
        var r = _engine.Execute("get users where age < 28", "testdb");

        Assert.Equal(1, r.Affected); // Charlie(22)
        Assert.Equal("Charlie", r.Data![0]["name"]);
    }

    // ── Less Than Or Equal (<=) ───────────────────────────────

    [Fact]
    public void Where_LessThanOrEqual()
    {
        var r = _engine.Execute("get users where age <= 28", "testdb");

        Assert.Equal(3, r.Affected); // Alice, Charlie, Diana
    }

    // ── Signed integer ────────────────────────────────────────

    [Fact]
    public void Where_SignedInt_GreaterThan()
    {
        var r = _engine.Execute("get users where score > 85", "testdb");

        Assert.Equal(2, r.Affected); // Bob(92), Diana(88)
    }

    [Fact]
    public void Where_SignedInt_Negative()
    {
        _engine.Execute("create table temps (value sint)", "testdb");
        _engine.Execute("upsert temps {value: -10}", "testdb");
        _engine.Execute("upsert temps {value: 5}", "testdb");
        _engine.Execute("upsert temps {value: -3}", "testdb");

        var r = _engine.Execute("get temps where value > -5", "testdb");

        Assert.Equal(2, r.Affected); // 5, -3
    }

    // ── Double ────────────────────────────────────────────────

    [Fact]
    public void Where_Double_GreaterThan()
    {
        var r = _engine.Execute("get users where rating > 4.0", "testdb");

        Assert.Equal(3, r.Affected); // Alice(4.5), Charlie(4.9), Diana(4.5)
    }

    [Fact]
    public void Where_Double_Equal()
    {
        var r = _engine.Execute("get users where rating = 4.5", "testdb");

        Assert.Equal(2, r.Affected); // Alice + Diana
    }

    // ── String comparison ─────────────────────────────────────

    [Fact]
    public void Where_String_GreaterThan()
    {
        var r = _engine.Execute("get users where name > 'Bob'", "testdb");

        Assert.Equal(2, r.Affected); // Charlie, Diana
    }

    [Fact]
    public void Where_String_LessThan()
    {
        var r = _engine.Execute("get users where name < 'Bob'", "testdb");

        Assert.Equal(1, r.Affected); // Alice
    }

    // ── ID filter ─────────────────────────────────────────────

    [Fact]
    public void Where_Id_Equal()
    {
        var r = _engine.Execute("get users where id = 2", "testdb");

        Assert.Equal(1, r.Affected);
        Assert.Equal("Bob", r.Data![0]["name"]);
    }

    [Fact]
    public void Where_Id_GreaterThan()
    {
        var r = _engine.Execute("get users where id > 2", "testdb");

        Assert.Equal(2, r.Affected); // Charlie(3), Diana(4)
    }

    // ── With select ───────────────────────────────────────────

    [Fact]
    public void Where_WithSelect()
    {
        var r = _engine.Execute("get users select name where age > 30", "testdb");

        Assert.Equal(1, r.Affected);
        Assert.Single(r.Data![0]); // only name
        Assert.Equal("Bob", r.Data[0]["name"]);
    }

    // ── Bool filter ───────────────────────────────────────────

    [Fact]
    public void Where_Bool()
    {
        _engine.Execute("create table flags (active bool default true)", "testdb");
        _engine.Execute("upsert flags {active: true}", "testdb");
        _engine.Execute("upsert flags {active: false}", "testdb");
        _engine.Execute("upsert flags {active: true}", "testdb");

        var r = _engine.Execute("get flags where active = true", "testdb");

        Assert.Equal(2, r.Affected);
    }

    // ── Null handling ─────────────────────────────────────────

    [Fact]
    public void Where_NullValues_Excluded()
    {
        _engine.Execute("upsert users {name: 'Eve'}", "testdb"); // age is null

        var r = _engine.Execute("get users where age > 0", "testdb");

        // Eve excluded because null never matches
        Assert.Equal(4, r.Affected); // original 4
    }

    // ── Error cases ───────────────────────────────────────────

    [Fact]
    public void Where_UnknownColumn_Error()
    {
        var r = _engine.Execute("get users where missing = 1", "testdb");

        Assert.Equal(SproutOperation.Error, r.Operation);
        Assert.Equal("UNKNOWN_COLUMN", r.Errors![0].Code);
    }

    [Fact]
    public void Where_CaseInsensitive()
    {
        var r = _engine.Execute("GET users WHERE age = 28", "testdb");

        Assert.Equal(SproutOperation.Get, r.Operation);
        Assert.Equal(2, r.Affected);
    }
}

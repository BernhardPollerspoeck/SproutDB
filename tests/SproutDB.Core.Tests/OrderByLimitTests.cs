namespace SproutDB.Core.Tests;

public class OrderByLimitTests : IDisposable
{
    private readonly string _tempDir;
    private readonly SproutEngine _engine;

    public OrderByLimitTests()
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

    private List<string> GetNames(SproutResponse r) =>
        r.Data!.Select(d => (string)d["name"]!).ToList();

    // ── ORDER BY ────────────────────────────────────────────

    [Fact]
    public void OrderBy_Name_Asc()
    {
        var r = _engine.Execute("get users order by name", "testdb");

        Assert.Equal(SproutOperation.Get, r.Operation);
        Assert.Equal(4, r.Affected);
        Assert.Equal(["Alice", "Bob", "Charlie", "Diana"], GetNames(r));
    }

    [Fact]
    public void OrderBy_Age_Desc()
    {
        var r = _engine.Execute("get users order by age desc", "testdb");

        Assert.Equal(4, r.Affected);
        var names = GetNames(r);
        // Bob(35) first, then Alice/Diana(28), Charlie(22) last
        Assert.Equal("Bob", names[0]);
        Assert.Equal("Charlie", names[3]);
    }

    [Fact]
    public void OrderBy_Multiple()
    {
        // order by age desc, name → Bob(35), Alice(28), Diana(28), Charlie(22)
        var r = _engine.Execute("get users order by age desc, name", "testdb");

        Assert.Equal(["Bob", "Alice", "Diana", "Charlie"], GetNames(r));
    }

    [Fact]
    public void OrderBy_With_Where()
    {
        var r = _engine.Execute("get users where age > 25 order by name", "testdb");

        Assert.Equal(3, r.Affected);
        Assert.Equal(["Alice", "Bob", "Diana"], GetNames(r));
    }

    [Fact]
    public void OrderBy_Id()
    {
        var r = _engine.Execute("get users order by _id desc", "testdb");

        Assert.Equal(["Diana", "Charlie", "Bob", "Alice"], GetNames(r));
    }

    [Fact]
    public void OrderBy_UnknownColumn_Error()
    {
        var r = _engine.Execute("get users order by missing", "testdb");

        Assert.Equal(SproutOperation.Error, r.Operation);
        Assert.Equal("UNKNOWN_COLUMN", r.Errors![0].Code);
    }

    [Fact]
    public void OrderBy_String()
    {
        var r = _engine.Execute("get users order by name", "testdb");

        Assert.Equal(["Alice", "Bob", "Charlie", "Diana"], GetNames(r));
    }

    // ── LIMIT ───────────────────────────────────────────────

    [Fact]
    public void Limit_Simple()
    {
        var r = _engine.Execute("get users limit 2", "testdb");

        Assert.Equal(2, r.Affected);
        Assert.Equal(2, r.Data!.Count);
    }

    [Fact]
    public void Limit_With_OrderBy()
    {
        var r = _engine.Execute("get users order by age desc limit 2", "testdb");

        Assert.Equal(2, r.Affected);
        // Bob(35) is first, then one of Alice/Diana(28)
        Assert.Equal("Bob", (string)r.Data![0]["name"]!);
    }

    [Fact]
    public void Limit_MoreThanRows()
    {
        var r = _engine.Execute("get users limit 100", "testdb");

        Assert.Equal(4, r.Affected);
    }

    [Fact]
    public void Limit_Zero()
    {
        var r = _engine.Execute("get users limit 0", "testdb");

        Assert.Equal(0, r.Affected);
        Assert.Empty(r.Data!);
    }

    [Fact]
    public void Limit_With_Where()
    {
        var r = _engine.Execute("get users where age > 25 limit 1", "testdb");

        Assert.Equal(1, r.Affected);
    }

    // ── COUNT ───────────────────────────────────────────────

    [Fact]
    public void Count_All()
    {
        var r = _engine.Execute("get users count", "testdb");

        Assert.Equal(SproutOperation.Get, r.Operation);
        Assert.Equal(4, r.Affected);
        Assert.Empty(r.Data!);
    }

    [Fact]
    public void Count_With_Where()
    {
        var r = _engine.Execute("get users where age > 25 count", "testdb");

        Assert.Equal(3, r.Affected);
        Assert.Empty(r.Data!);
    }

    [Fact]
    public void Count_NoMatch()
    {
        var r = _engine.Execute("get users where age > 100 count", "testdb");

        Assert.Equal(0, r.Affected);
        Assert.Empty(r.Data!);
    }

    // ── DISTINCT ────────────────────────────────────────────

    [Fact]
    public void Distinct_Values()
    {
        var r = _engine.Execute("get users select age distinct", "testdb");

        Assert.Equal(SproutOperation.Get, r.Operation);
        Assert.Equal(3, r.Affected); // 22, 28, 35
    }

    [Fact]
    public void Distinct_With_Where()
    {
        var r = _engine.Execute("get users select age distinct where age > 25", "testdb");

        Assert.Equal(2, r.Affected); // 28, 35
    }

    [Fact]
    public void Distinct_String()
    {
        var r = _engine.Execute("get users select name distinct", "testdb");

        Assert.Equal(4, r.Affected); // all unique
    }

    [Fact]
    public void Distinct_Count()
    {
        var r = _engine.Execute("get users select age distinct count", "testdb");

        Assert.Equal(3, r.Affected); // 3 unique ages
        Assert.Empty(r.Data!);
    }
}

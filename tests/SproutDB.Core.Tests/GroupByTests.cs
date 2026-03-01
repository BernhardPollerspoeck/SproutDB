namespace SproutDB.Core.Tests;

public class GroupByTests : IDisposable
{
    private readonly string _tempDir;
    private readonly SproutEngine _engine;

    public GroupByTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), $"sproutdb-test-{Guid.NewGuid()}");
        _engine = new SproutEngine(_tempDir);
        _engine.Execute("create database", "testdb");

        _engine.Execute(
            "create table orders (city string 50, status string 30, amount double, customer_id uint)",
            "testdb");

        // Berlin: 2 completed, 1 pending
        _engine.Execute("upsert orders {city: 'Berlin', status: 'completed', amount: 100.0, customer_id: 1}", "testdb");
        _engine.Execute("upsert orders {city: 'Berlin', status: 'completed', amount: 200.0, customer_id: 2}", "testdb");
        _engine.Execute("upsert orders {city: 'Berlin', status: 'pending', amount: 50.0, customer_id: 1}", "testdb");

        // Munich: 1 completed, 1 cancelled
        _engine.Execute("upsert orders {city: 'Munich', status: 'completed', amount: 150.0, customer_id: 3}", "testdb");
        _engine.Execute("upsert orders {city: 'Munich', status: 'cancelled', amount: 75.0, customer_id: 3}", "testdb");

        // Hamburg: 1 completed
        _engine.Execute("upsert orders {city: 'Hamburg', status: 'completed', amount: 300.0, customer_id: 2}", "testdb");
    }

    public void Dispose()
    {
        _engine.Dispose();
        if (Directory.Exists(_tempDir))
            Directory.Delete(_tempDir, true);
    }

    // ── #066: aggregate + group by ──────────────────────────

    [Fact]
    public void Sum_GroupBy_Status()
    {
        var r = _engine.Execute("get orders sum amount as revenue group by status", "testdb");

        Assert.Equal(SproutOperation.Get, r.Operation);
        Assert.NotNull(r.Data);
        Assert.Equal(3, r.Data.Count); // completed, pending, cancelled

        var completed = r.Data.First(row => (string?)row["status"] == "completed");
        Assert.Equal(750.0, (double)completed["revenue"]!);

        var pending = r.Data.First(row => (string?)row["status"] == "pending");
        Assert.Equal(50.0, (double)pending["revenue"]!);

        var cancelled = r.Data.First(row => (string?)row["status"] == "cancelled");
        Assert.Equal(75.0, (double)cancelled["revenue"]!);
    }

    [Fact]
    public void Sum_GroupBy_City()
    {
        var r = _engine.Execute("get orders sum amount group by city", "testdb");

        Assert.NotNull(r.Data);
        Assert.Equal(3, r.Data.Count);

        var berlin = r.Data.First(row => (string?)row["city"] == "Berlin");
        Assert.Equal(350.0, (double)berlin["sum"]!); // default alias = "sum"

        var hamburg = r.Data.First(row => (string?)row["city"] == "Hamburg");
        Assert.Equal(300.0, (double)hamburg["sum"]!);
    }

    [Fact]
    public void Min_GroupBy()
    {
        var r = _engine.Execute("get orders min amount as cheapest group by city", "testdb");

        Assert.NotNull(r.Data);
        var berlin = r.Data.First(row => (string?)row["city"] == "Berlin");
        Assert.Equal(50.0, (double)berlin["cheapest"]!);
    }

    [Fact]
    public void Max_GroupBy()
    {
        var r = _engine.Execute("get orders max amount as most_expensive group by city", "testdb");

        Assert.NotNull(r.Data);
        var berlin = r.Data.First(row => (string?)row["city"] == "Berlin");
        Assert.Equal(200.0, (double)berlin["most_expensive"]!);
    }

    [Fact]
    public void Avg_GroupBy()
    {
        var r = _engine.Execute("get orders avg amount as avg_amount group by status", "testdb");

        Assert.NotNull(r.Data);
        var completed = r.Data.First(row => (string?)row["status"] == "completed");
        // (100 + 200 + 150 + 300) / 4 = 187.5
        Assert.Equal(187.5, (double?)completed["avg_amount"]);
    }

    // ── #067: count + group by ──────────────────────────────

    [Fact]
    public void Count_GroupBy_City()
    {
        var r = _engine.Execute("get orders count group by city", "testdb");

        Assert.NotNull(r.Data);
        Assert.Equal(3, r.Data.Count);

        var berlin = r.Data.First(row => (string?)row["city"] == "Berlin");
        Assert.Equal(3, (int)berlin["count"]!);

        var munich = r.Data.First(row => (string?)row["city"] == "Munich");
        Assert.Equal(2, (int)munich["count"]!);

        var hamburg = r.Data.First(row => (string?)row["city"] == "Hamburg");
        Assert.Equal(1, (int)hamburg["count"]!);
    }

    [Fact]
    public void Count_GroupBy_Status()
    {
        var r = _engine.Execute("get orders count group by status", "testdb");

        Assert.NotNull(r.Data);
        Assert.Equal(3, r.Data.Count);

        var completed = r.Data.First(row => (string?)row["status"] == "completed");
        Assert.Equal(4, (int)completed["count"]!);
    }

    [Fact]
    public void Count_GroupBy_MultipleColumns()
    {
        var r = _engine.Execute("get orders count group by city, status", "testdb");

        Assert.NotNull(r.Data);
        // Berlin/completed, Berlin/pending, Munich/completed, Munich/cancelled, Hamburg/completed = 5
        Assert.Equal(5, r.Data.Count);

        var berlinCompleted = r.Data.First(row =>
            (string?)row["city"] == "Berlin" && (string?)row["status"] == "completed");
        Assert.Equal(2, (int)berlinCompleted["count"]!);
    }

    // ── #068: group by + order by + limit ───────────────────

    [Fact]
    public void Avg_GroupBy_OrderBy_Limit()
    {
        var r = _engine.Execute(
            "get orders avg amount as avg_amount group by customer_id order by avg_amount desc limit 2",
            "testdb");

        Assert.NotNull(r.Data);
        Assert.Equal(2, r.Data.Count);

        // Customer 2: (200 + 300) / 2 = 250 → highest
        // Customer 3: (150 + 75) / 2 = 112.5
        // Customer 1: (100 + 50) / 2 = 75 → lowest
        Assert.Equal(250.0, (double?)r.Data[0]["avg_amount"]);
    }

    [Fact]
    public void Count_GroupBy_OrderBy_Count_Desc()
    {
        var r = _engine.Execute(
            "get orders count group by city order by count desc",
            "testdb");

        Assert.NotNull(r.Data);
        Assert.Equal(3, r.Data.Count);

        // Berlin: 3, Munich: 2, Hamburg: 1
        Assert.Equal("Berlin", (string?)r.Data[0]["city"]);
        Assert.Equal("Hamburg", (string?)r.Data[2]["city"]);
    }

    [Fact]
    public void Sum_GroupBy_WithWhere()
    {
        var r = _engine.Execute(
            "get orders sum amount as revenue where status != 'cancelled' group by city",
            "testdb");

        Assert.NotNull(r.Data);

        // Munich: only completed (150), cancelled excluded
        var munich = r.Data.First(row => (string?)row["city"] == "Munich");
        Assert.Equal(150.0, (double)munich["revenue"]!);
    }

    [Fact]
    public void Sum_GroupBy_WithWhere_OrderBy_Limit()
    {
        var r = _engine.Execute(
            "get orders sum amount as revenue where status = 'completed' group by city order by revenue desc limit 2",
            "testdb");

        Assert.NotNull(r.Data);
        Assert.Equal(2, r.Data.Count);

        // Completed: Berlin=300, Hamburg=300, Munich=150 → top 2
        Assert.True((double)r.Data[0]["revenue"]! >= (double)r.Data[1]["revenue"]!);
    }

    // ── Error cases ─────────────────────────────────────────

    [Fact]
    public void GroupBy_UnknownColumn_Error()
    {
        var r = _engine.Execute("get orders count group by nonexistent", "testdb");

        Assert.Equal(SproutOperation.Error, r.Operation);
        Assert.NotNull(r.Errors);
        Assert.Equal("UNKNOWN_COLUMN", r.Errors[0].Code);
    }
}

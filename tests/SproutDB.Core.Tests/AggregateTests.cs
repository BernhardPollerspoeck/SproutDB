namespace SproutDB.Core.Tests;

public class AggregateTests : IDisposable
{
    private readonly string _tempDir;
    private readonly SproutEngine _engine;

    public AggregateTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), $"sproutdb-test-{Guid.NewGuid()}");
        _engine = new SproutEngine(_tempDir);
        _engine.ExecuteOne("create database", "testdb");
        _engine.ExecuteOne(
            "create table orders (product string 100, amount double, quantity uint, status string 50)",
            "testdb");

        // Seed orders
        _engine.ExecuteOne("upsert orders {product: 'Widget', amount: 25.50, quantity: 10, status: 'completed'}", "testdb");
        _engine.ExecuteOne("upsert orders {product: 'Gadget', amount: 75.00, quantity: 5, status: 'completed'}", "testdb");
        _engine.ExecuteOne("upsert orders {product: 'Doohickey', amount: 12.25, quantity: 20, status: 'pending'}", "testdb");
        _engine.ExecuteOne("upsert orders {product: 'Thingamajig', amount: 50.00, quantity: 3, status: 'completed'}", "testdb");
        _engine.ExecuteOne("upsert orders {product: 'Whatchamacallit', amount: 37.25, quantity: 8, status: 'pending'}", "testdb");
    }

    public void Dispose()
    {
        _engine.Dispose();
        if (Directory.Exists(_tempDir))
            Directory.Delete(_tempDir, true);
    }

    // ── SUM (#059) ──────────────────────────────────────────

    [Fact]
    public void Sum_All()
    {
        var r = _engine.ExecuteOne("get orders sum amount", "testdb");

        Assert.Equal(SproutOperation.Get, r.Operation);
        Assert.Equal(1, r.Affected);
        Assert.Single(r.Data!);
        Assert.Equal(200.0, (double)r.Data![0]["sum"]!);
    }

    [Fact]
    public void Sum_WithAlias()
    {
        var r = _engine.ExecuteOne("get orders sum amount as total_revenue", "testdb");

        Assert.True(r.Data![0].ContainsKey("total_revenue"));
        Assert.Equal(200.0, (double)r.Data[0]["total_revenue"]!);
    }

    [Fact]
    public void Sum_WithWhere()
    {
        // completed: 25.50 + 75.00 + 50.00 = 150.50
        var r = _engine.ExecuteOne("get orders sum amount where status = 'completed'", "testdb");

        Assert.Equal(150.50, (double)r.Data![0]["sum"]!);
    }

    [Fact]
    public void Sum_Integer()
    {
        // quantity: 10 + 5 + 20 + 3 + 8 = 46
        var r = _engine.ExecuteOne("get orders sum quantity", "testdb");

        Assert.Equal(46.0, (double)r.Data![0]["sum"]!);
    }

    [Fact]
    public void Sum_OnStringColumn_Error()
    {
        var r = _engine.ExecuteOne("get orders sum product", "testdb");

        Assert.Equal(SproutOperation.Error, r.Operation);
        Assert.Equal("TYPE_MISMATCH", r.Errors![0].Code);
        Assert.Contains("numeric", r.Errors[0].Message);
    }

    [Fact]
    public void Sum_UnknownColumn_Error()
    {
        var r = _engine.ExecuteOne("get orders sum missing", "testdb");

        Assert.Equal(SproutOperation.Error, r.Operation);
        Assert.Equal("UNKNOWN_COLUMN", r.Errors![0].Code);
    }

    // ── AVG (#060) ──────────────────────────────────────────

    [Fact]
    public void Avg_All()
    {
        // (25.50 + 75.00 + 12.25 + 50.00 + 37.25) / 5 = 40.0
        var r = _engine.ExecuteOne("get orders avg amount", "testdb");

        Assert.Equal(40.0, (double)r.Data![0]["avg"]!);
    }

    [Fact]
    public void Avg_WithAlias()
    {
        var r = _engine.ExecuteOne("get orders avg amount as average_order_value", "testdb");

        Assert.Equal(40.0, (double)r.Data![0]["average_order_value"]!);
    }

    [Fact]
    public void Avg_WithWhere()
    {
        // completed: (25.50 + 75.00 + 50.00) / 3 ≈ 50.1667
        var r = _engine.ExecuteOne("get orders avg amount where status = 'completed'", "testdb");

        var avg = (double)r.Data![0]["avg"]!;
        Assert.True(Math.Abs(avg - 50.166666666666664) < 0.001);
    }

    [Fact]
    public void Avg_NoMatch_Null()
    {
        var r = _engine.ExecuteOne("get orders avg amount where status = 'cancelled'", "testdb");

        Assert.Null(r.Data![0]["avg"]);
    }

    [Fact]
    public void Avg_OnStringColumn_Error()
    {
        var r = _engine.ExecuteOne("get orders avg product", "testdb");

        Assert.Equal(SproutOperation.Error, r.Operation);
        Assert.Equal("TYPE_MISMATCH", r.Errors![0].Code);
    }

    // ── MIN (#061) ──────────────────────────────────────────

    [Fact]
    public void Min_Numeric()
    {
        var r = _engine.ExecuteOne("get orders min amount", "testdb");

        Assert.Equal(12.25, (double)r.Data![0]["min"]!);
    }

    [Fact]
    public void Min_String()
    {
        var r = _engine.ExecuteOne("get orders min product", "testdb");

        Assert.Equal("Doohickey", (string)r.Data![0]["min"]!);
    }

    [Fact]
    public void Min_WithWhere()
    {
        // completed amounts: 25.50, 75.00, 50.00 → min = 25.50
        var r = _engine.ExecuteOne("get orders min amount where status = 'completed'", "testdb");

        Assert.Equal(25.50, (double)r.Data![0]["min"]!);
    }

    [Fact]
    public void Min_NoMatch_Null()
    {
        var r = _engine.ExecuteOne("get orders min amount where status = 'cancelled'", "testdb");

        Assert.Null(r.Data![0]["min"]);
    }

    // ── MAX (#062) ──────────────────────────────────────────

    [Fact]
    public void Max_Numeric()
    {
        var r = _engine.ExecuteOne("get orders max amount", "testdb");

        Assert.Equal(75.0, (double)r.Data![0]["max"]!);
    }

    [Fact]
    public void Max_String()
    {
        var r = _engine.ExecuteOne("get orders max product", "testdb");

        Assert.Equal("Widget", (string)r.Data![0]["max"]!);
    }

    [Fact]
    public void Max_WithWhere()
    {
        // pending amounts: 12.25, 37.25 → max = 37.25
        var r = _engine.ExecuteOne("get orders max amount where status = 'pending'", "testdb");

        Assert.Equal(37.25, (double)r.Data![0]["max"]!);
    }

    // ── Alias + WHERE combined (#063, #064) ─────────────────

    [Fact]
    public void Sum_Alias_Where()
    {
        var r = _engine.ExecuteOne("get orders sum amount as total_revenue where status = 'completed'", "testdb");

        Assert.True(r.Data![0].ContainsKey("total_revenue"));
        Assert.Equal(150.50, (double)r.Data[0]["total_revenue"]!);
    }

    [Fact]
    public void Avg_Alias_Where()
    {
        var r = _engine.ExecuteOne("get orders avg amount as average_order_value where status = 'completed'", "testdb");

        Assert.True(r.Data![0].ContainsKey("average_order_value"));
        var avg = (double)r.Data[0]["average_order_value"]!;
        Assert.True(Math.Abs(avg - 50.166666666666664) < 0.001);
    }

    // ── Aggregation + DateTime WHERE (#078) ─────────────────

    [Fact]
    public void Avg_Where_DateTime()
    {
        _engine.ExecuteOne("create table sales (revenue double, created datetime)", "testdb");
        _engine.ExecuteOne("upsert sales {revenue: 100.0, created: '2024-06-15 10:00:00'}", "testdb");
        _engine.ExecuteOne("upsert sales {revenue: 200.0, created: '2025-03-01 14:00:00'}", "testdb");
        _engine.ExecuteOne("upsert sales {revenue: 300.0, created: '2025-07-20 08:00:00'}", "testdb");

        // avg of sales after 2025-01-01: (200 + 300) / 2 = 250
        var r = _engine.ExecuteOne("get sales avg revenue where created > '2025-01-01 00:00:00'", "testdb");

        Assert.Equal(250.0, (double)r.Data![0]["avg"]!);
    }

    // ── Null handling ───────────────────────────────────────

    [Fact]
    public void Sum_SkipsNulls()
    {
        _engine.ExecuteOne("upsert orders {product: 'Nullie', status: 'void'}", "testdb"); // amount is null

        // Sum should still be 200.0 (null excluded)
        var r = _engine.ExecuteOne("get orders sum amount", "testdb");

        Assert.Equal(200.0, (double)r.Data![0]["sum"]!);
    }

    [Fact]
    public void Avg_SkipsNulls()
    {
        _engine.ExecuteOne("upsert orders {product: 'Nullie', status: 'void'}", "testdb"); // amount is null

        // Avg should still be 40.0 (null excluded, count = 5)
        var r = _engine.ExecuteOne("get orders avg amount", "testdb");

        Assert.Equal(40.0, (double)r.Data![0]["avg"]!);
    }

    // ── Aggregate on ID ─────────────────────────────────────

    [Fact]
    public void Max_Id()
    {
        var r = _engine.ExecuteOne("get orders max _id", "testdb");

        Assert.Equal(5ul, (ulong)r.Data![0]["max"]!);
    }

    [Fact]
    public void Min_Id()
    {
        var r = _engine.ExecuteOne("get orders min _id", "testdb");

        Assert.Equal(1ul, (ulong)r.Data![0]["min"]!);
    }
}

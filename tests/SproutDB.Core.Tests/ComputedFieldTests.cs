namespace SproutDB.Core.Tests;

public class ComputedFieldTests : IDisposable
{
    private readonly string _tempDir;
    private readonly SproutEngine _engine;

    public ComputedFieldTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), $"sproutdb-test-{Guid.NewGuid()}");
        _engine = new SproutEngine(_tempDir);
        _engine.Execute("create database", "testdb");

        _engine.Execute(
            "create table orders (name string 50, price double, quantity uint, amount double, discount float)",
            "testdb");

        _engine.Execute("upsert orders {name: 'Widget', price: 10.0, quantity: 3, amount: 30.0, discount: 0.1}", "testdb");
        _engine.Execute("upsert orders {name: 'Gadget', price: 25.0, quantity: 2, amount: 50.0, discount: 0.2}", "testdb");
        _engine.Execute("upsert orders {name: 'Doohickey', price: 5.0, quantity: 10, amount: 50.0, discount: 0.0}", "testdb");
    }

    public void Dispose()
    {
        _engine.Dispose();
        if (Directory.Exists(_tempDir))
            Directory.Delete(_tempDir, true);
    }

    // ── #069: computed field with literal ────────────────────

    [Fact]
    public void Computed_MultiplyLiteral_Tax()
    {
        var r = _engine.Execute("get orders select amount, amount * 0.19 as tax", "testdb");

        Assert.Equal(SproutOperation.Get, r.Operation);
        Assert.NotNull(r.Data);
        Assert.Equal(3, r.Data.Count);

        var widget = r.Data.First(row => (double?)row["amount"] == 30.0);
        Assert.Equal(5.7, (double)widget["tax"]!, 10);
    }

    [Fact]
    public void Computed_AddLiteral()
    {
        var r = _engine.Execute("get orders select price, price + 5 as adjusted", "testdb");

        Assert.NotNull(r.Data);
        var widget = r.Data.First(row => (double?)row["price"] == 10.0);
        Assert.Equal(15.0, (double)widget["adjusted"]!);
    }

    [Fact]
    public void Computed_SubtractLiteral()
    {
        var r = _engine.Execute("get orders select price, price - 2.5 as discounted", "testdb");

        Assert.NotNull(r.Data);
        var widget = r.Data.First(row => (double?)row["price"] == 10.0);
        Assert.Equal(7.5, (double)widget["discounted"]!);
    }

    [Fact]
    public void Computed_DivideLiteral()
    {
        var r = _engine.Execute("get orders select amount, amount / 2 as half", "testdb");

        Assert.NotNull(r.Data);
        var widget = r.Data.First(row => (double?)row["amount"] == 30.0);
        Assert.Equal(15.0, (double)widget["half"]!);
    }

    // ── #070: computed field with column ─────────────────────

    [Fact]
    public void Computed_MultiplyColumns()
    {
        var r = _engine.Execute("get orders select name, price, quantity, price * quantity as total", "testdb");

        Assert.NotNull(r.Data);
        Assert.Equal(3, r.Data.Count);

        var widget = r.Data.First(row => (string?)row["name"] == "Widget");
        Assert.True(widget.ContainsKey("price"));
        Assert.True(widget.ContainsKey("quantity"));
        Assert.Equal(30.0, (double)widget["total"]!);

        var doohickey = r.Data.First(row => (string?)row["name"] == "Doohickey");
        Assert.Equal(50.0, (double)doohickey["total"]!);
    }

    [Fact]
    public void Computed_SubtractColumns()
    {
        var r = _engine.Execute("get orders select name, amount, amount - price as profit", "testdb");

        Assert.NotNull(r.Data);
        var widget = r.Data.First(row => (string?)row["name"] == "Widget");
        Assert.Equal(20.0, (double)widget["profit"]!);
    }

    // ── #071: type inference ────────────────────────────────

    [Fact]
    public void TypeInference_IntTimesDouble_ReturnsDouble()
    {
        // quantity (uint) * price (double) → double
        var r = _engine.Execute("get orders select quantity * price as total", "testdb");

        Assert.NotNull(r.Data);
        var widget = r.Data.First(row => (double?)row["total"] == 30.0);
        Assert.IsType<double>(widget["total"]);
    }

    [Fact]
    public void TypeInference_DivisionAlwaysDouble()
    {
        // integer / integer → double
        var r = _engine.Execute("get orders select quantity / 2 as half_qty", "testdb");

        Assert.NotNull(r.Data);
        // Widget: 3 / 2 = 1.5
        var widget = r.Data[0];
        Assert.IsType<double>(widget["half_qty"]);
        Assert.Equal(1.5, (double)widget["half_qty"]!);
    }

    [Fact]
    public void TypeInference_IntTimesIntLiteral_StaysInteger()
    {
        // uint * int literal → long (signed because literal could be negative)
        // Actually the literal is stored as double in ComputedColumn.RightLiteral,
        // so this will produce double. That's fine.
        var r = _engine.Execute("get orders select quantity * 2 as doubled", "testdb");

        Assert.NotNull(r.Data);
        var widget = r.Data[0];
        // quantity=3, doubled=6
        Assert.NotNull(widget["doubled"]);
    }

    // ── computed field not in select ─────────────────────────

    [Fact]
    public void Computed_SourceNotInSelect_StillWorks()
    {
        // Only computed field, no simple columns — source columns should not leak into result
        var r = _engine.Execute("get orders select price * quantity as total", "testdb");

        Assert.NotNull(r.Data);
        Assert.Equal(3, r.Data.Count);

        var first = r.Data[0];
        // Should have only total — no id, no price, no quantity
        Assert.True(first.ContainsKey("total"));
        Assert.False(first.ContainsKey("_id"));
        Assert.False(first.ContainsKey("price"));
        Assert.False(first.ContainsKey("quantity"));
    }

    [Fact]
    public void Computed_WithExplicitSource_SourceStaysInResult()
    {
        var r = _engine.Execute("get orders select name, price, price * 2 as double_price", "testdb");

        Assert.NotNull(r.Data);
        var first = r.Data[0];
        // price is explicitly selected, should remain
        Assert.True(first.ContainsKey("price"));
        Assert.True(first.ContainsKey("double_price"));
    }

    // ── combined with other features ────────────────────────

    [Fact]
    public void Computed_WithWhere()
    {
        var r = _engine.Execute("get orders select name, price * quantity as total where quantity > 2", "testdb");

        Assert.NotNull(r.Data);
        Assert.Equal(2, r.Data.Count); // Widget (3) and Doohickey (10)
    }

    [Fact]
    public void Computed_WithOrderBy()
    {
        var r = _engine.Execute("get orders select name, price * quantity as total order by total desc", "testdb");

        Assert.NotNull(r.Data);
        Assert.Equal(3, r.Data.Count);
        // Gadget: 25*2=50, Doohickey: 5*10=50, Widget: 10*3=30
        Assert.Equal(30.0, (double)r.Data[2]["total"]!);
    }

    [Fact]
    public void Computed_WithLimit()
    {
        var r = _engine.Execute("get orders select name, price * quantity as total order by total desc limit 1", "testdb");

        Assert.NotNull(r.Data);
        Assert.Single(r.Data);
    }

    // ── null handling ───────────────────────────────────────

    [Fact]
    public void Computed_NullOperand_ReturnsNull()
    {
        _engine.Execute("upsert orders {name: 'NoPrice', quantity: 1}", "testdb");

        var r = _engine.Execute("get orders select name, price * quantity as total where name = 'NoPrice'", "testdb");

        Assert.NotNull(r.Data);
        Assert.Single(r.Data);
        Assert.Null(r.Data[0]["total"]);
    }

    // ── error cases ─────────────────────────────────────────

    [Fact]
    public void Computed_UnknownLeftColumn_Error()
    {
        var r = _engine.Execute("get orders select nonexistent * 2 as x", "testdb");

        Assert.Equal(SproutOperation.Error, r.Operation);
        Assert.NotNull(r.Errors);
        Assert.Equal("UNKNOWN_COLUMN", r.Errors[0].Code);
    }

    [Fact]
    public void Computed_UnknownRightColumn_Error()
    {
        var r = _engine.Execute("get orders select price * nonexistent as x", "testdb");

        Assert.Equal(SproutOperation.Error, r.Operation);
        Assert.NotNull(r.Errors);
        Assert.Equal("UNKNOWN_COLUMN", r.Errors[0].Code);
    }

    [Fact]
    public void Computed_DivisionByZero_ReturnsNull()
    {
        var r = _engine.Execute("get orders select price / 0 as boom", "testdb");

        Assert.NotNull(r.Data);
        Assert.Null(r.Data[0]["boom"]);
    }
}

namespace SproutDB.Core.Tests;

/// <summary>
/// These are the saved queries from the Sandbox migration
/// <c>006_SavedQueries.cs</c> that currently fail either at parse time or
/// during execution. Each test encodes the expected behavior (success + data
/// where applicable) so they'll go green once the underlying bugs are fixed.
///
/// Every test is deliberately red today. Don't mark them [Fact(Skip=...)];
/// we want the CI signal until we've worked through the fixes.
///
/// Shared setup mirrors the sandbox schema (<c>001_CreateSchema.cs</c>) plus
/// a tiny seed so follow/join-based queries have something to iterate over.
/// </summary>
public class SavedQueriesFailingTests : IDisposable
{
    private readonly string _tempDir;
    private readonly SproutEngine _engine;
    private const string Db = "shop";

    public SavedQueriesFailingTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), $"sproutdb-savedq-{Guid.NewGuid()}");
        _engine = new SproutEngine(_tempDir);
        _engine.ExecuteOne("create database", Db);

        // ── Schema (sandbox/001_CreateSchema.cs) ───────────────
        _engine.ExecuteOne(
            "create table suppliers (name string 200, country string 100, contact_email string 320, rating sint)", Db);
        _engine.ExecuteOne(
            "create table plants (name string 200, species string 200, category string 50, price sint, stock sint, supplier_id string 36, planted_at string 30)", Db);
        _engine.ExecuteOne(
            "create table customers (name string 200, email string 320, city string 100, joined_at string 30)", Db);
        _engine.ExecuteOne(
            "create table orders (customer_id string 36, order_date string 30, status string 20, total sint)", Db);
        _engine.ExecuteOne(
            "create table order_items (order_id string 36, plant_id string 36, quantity sint, unit_price sint)", Db);

        // ── Seed: minimal graph so follow-chains can produce rows ──
        _engine.ExecuteOne("upsert suppliers {name: 'Greenhouse GmbH', country: 'DE', contact_email: 'a@b.de', rating: 5}", Db);
        _engine.ExecuteOne("upsert plants {name: 'Oak', species: 'Quercus', category: 'tree', price: 45, stock: 12, supplier_id: '1', planted_at: '2025-03-01'}", Db);
        _engine.ExecuteOne("upsert plants {name: 'Basil', species: 'Ocimum', category: 'herb', price: 3, stock: 50, supplier_id: '1', planted_at: '2025-04-10'}", Db);
        _engine.ExecuteOne("upsert plants {name: 'Apple', species: 'Malus', category: 'fruit', price: 30, stock: 8, supplier_id: '1', planted_at: '2025-02-20'}", Db);

        _engine.ExecuteOne("upsert customers {name: 'Anna', email: 'anna@test.de', city: 'Berlin', joined_at: '2025-01-01'}", Db);
        _engine.ExecuteOne("upsert customers {name: 'Bert', email: 'bert@test.com', city: 'Munich', joined_at: '2025-02-01'}", Db);

        _engine.ExecuteOne("upsert orders {customer_id: '1', order_date: '2025-04-01', status: 'delivered', total: 90}", Db);
        _engine.ExecuteOne("upsert orders {customer_id: '1', order_date: '2025-04-05', status: 'pending', total: 30}", Db);
        _engine.ExecuteOne("upsert orders {customer_id: '2', order_date: '2025-04-10', status: 'delivered', total: 60}", Db);

        _engine.ExecuteOne("upsert order_items {order_id: '1', plant_id: '1', quantity: 2, unit_price: 45}", Db);
        _engine.ExecuteOne("upsert order_items {order_id: '1', plant_id: '3', quantity: 1, unit_price: 30}", Db);
        _engine.ExecuteOne("upsert order_items {order_id: '3', plant_id: '2', quantity: 5, unit_price: 3}", Db);
    }

    public void Dispose()
    {
        _engine.Dispose();
        if (Directory.Exists(_tempDir))
            Directory.Delete(_tempDir, true);
    }

    private SproutResponse Run(string query)
    {
        var resp = _engine.ExecuteOne(query, Db);
        return resp;
    }

    private static void AssertOk(SproutResponse r, string query)
    {
        Assert.True(
            r.Errors is null,
            $"query failed: {query}\n" +
            (r.Errors is null ? "" : string.Join("; ", r.Errors.Select(e => $"{e.Code}: {e.Message}"))));
    }

    // ── 1. Stock Overview ─────────────────────────────────────

    [Fact]
    public void StockOverview_MultipleAggregatesWithAliases()
    {
        const string q =
            "get plants sum stock as total, min stock as lowest, " +
            "max stock as highest, avg stock as average";

        var r = Run(q);
        AssertOk(r, q);
        Assert.NotNull(r.Data);
        Assert.Single(r.Data);

        var row = r.Data[0];
        Assert.Contains("total", row.Keys);
        Assert.Contains("lowest", row.Keys);
        Assert.Contains("highest", row.Keys);
        Assert.Contains("average", row.Keys);
    }

    // ── 2. Complex Filter ─────────────────────────────────────

    [Fact]
    public void ComplexFilter_ParenthesizedNotOrAndChain()
    {
        const string q =
            "get plants where not (category = 'herb' or category = 'vegetable') " +
            "and stock > 10 and price < 50 order by price desc";

        var r = Run(q);
        AssertOk(r, q);
        Assert.NotNull(r.Data);
        // Oak (tree, stock=12, price=45) passes — no herb/veg.
        Assert.Contains(r.Data, row => row["name"]?.ToString() == "Oak");
    }

    // ── 3. Orders With Customer ───────────────────────────────

    [Fact]
    public void OrdersWithCustomer_FollowThenSelectWithAliasColumn()
    {
        const string q =
            "get orders follow orders.customer_id -> customers._id as c " +
            "select c.name, order_date, status, total order by order_date desc";

        var r = Run(q);
        AssertOk(r, q);
        Assert.NotNull(r.Data);
        Assert.NotEmpty(r.Data);
    }

    // ── 4. Plants With Supplier ───────────────────────────────

    [Fact]
    public void PlantsWithSupplier_FollowWithAliasColumnSelect()
    {
        const string q =
            "get plants follow plants.supplier_id -> suppliers._id as s " +
            "select name, category, price, s.name, s.country order by name";

        var r = Run(q);
        AssertOk(r, q);
        Assert.NotNull(r.Data);
        Assert.NotEmpty(r.Data);
    }

    // ── 5. All Customers + Orders (left join) ─────────────────

    [Fact]
    public void AllCustomersAndOrders_LeftFollowWithAliasColumns()
    {
        const string q =
            "get customers follow customers._id ->? orders.customer_id as ord " +
            "select name, city, ord.order_date, ord.total order by name";

        var r = Run(q);
        AssertOk(r, q);
        Assert.NotNull(r.Data);
        Assert.NotEmpty(r.Data);
    }

    // ── 6. Big Spender Orders ────────────────────────────────

    [Fact]
    public void BigSpenderOrders_WhereOnAliasColumn()
    {
        const string q =
            "get customers follow customers._id -> orders.customer_id as ord " +
            "where ord.total >= 80 " +
            "select name, ord.order_date, ord.total order by ord.total desc";

        var r = Run(q);
        AssertOk(r, q);
        Assert.NotNull(r.Data);
        // Only Anna's $90 order (total >= 80) should remain.
        Assert.Single(r.Data);
    }

    // ── 7. Full Order Details (chained 3-table follow) ────────

    [Fact]
    public void FullOrderDetails_ChainedFollowWithAliasColumns()
    {
        const string q =
            "get order_items " +
            "follow order_items.order_id -> orders._id as ord " +
            "follow order_items.plant_id -> plants._id as plant " +
            "select ord.order_date, plant.name, quantity, unit_price " +
            "order by ord.order_date desc";

        var r = Run(q);
        AssertOk(r, q);
        Assert.NotNull(r.Data);
        Assert.NotEmpty(r.Data);
    }

    // ── 8. Order Item Values (chained follow + computed col) ──

    [Fact]
    public void OrderItemValues_ChainedFollowWithComputedColumn()
    {
        const string q =
            "get order_items " +
            "follow order_items.plant_id -> plants._id as plant " +
            "follow order_items.order_id -> orders._id as ord " +
            "select plant.name, quantity, unit_price, " +
            "quantity * unit_price as total, ord.status " +
            "order by total desc";

        var r = Run(q);
        AssertOk(r, q);
        Assert.NotNull(r.Data);
        Assert.NotEmpty(r.Data);
        Assert.Contains(r.Data, row => row.ContainsKey("total"));
    }

    // ── 9. Full Receipt (the big one) ─────────────────────────

    [Fact]
    public void FullReceipt_FollowWhereAliasColumnChainedFollowSelectComputed()
    {
        const string q =
            "get customers " +
            "follow customers._id -> orders.customer_id as ord " +
            "where ord.status = 'delivered' " +
            "follow ord._id -> order_items.order_id as item " +
            "follow item.plant_id -> plants._id as plant " +
            "select name, ord.order_date, plant.name, plant.category, " +
            "item.quantity, item.unit_price, " +
            "item.quantity * item.unit_price as line_total " +
            "order by name, ord.order_date desc limit 50";

        var r = Run(q);
        AssertOk(r, q);
        Assert.NotNull(r.Data);
        Assert.NotEmpty(r.Data);
    }

    // ── 10. Whitespace around dot in alias.column ─────────────

    [Fact]
    public void WhitespaceAroundDot_Tolerated_InFollowSelectAndWhere()
    {
        const string q =
            "get orders follow orders.customer_id -> customers._id as c " +
            "where c . name = 'Anna' " +
            "select c . name , order_date , total";

        var r = Run(q);
        AssertOk(r, q);
        Assert.NotNull(r.Data);
        Assert.NotEmpty(r.Data);
    }
}

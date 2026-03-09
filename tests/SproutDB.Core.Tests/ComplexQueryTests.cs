namespace SproutDB.Core.Tests;

public class ComplexQueryTests : IDisposable
{
    private readonly string _tempDir;
    private readonly SproutEngine _engine;
    private const string Db = "garden";

    public ComplexQueryTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), $"sproutdb-test-{Guid.NewGuid()}");
        _engine = new SproutEngine(_tempDir);
        _engine.Execute("create database", Db);

        // Mirror the sandbox garden schema
        _engine.Execute("create table customers (name string 200, email string 320, city string 100, joined_at string 30)", Db);
        _engine.Execute("create table orders (customer_id string 36, order_date string 30, status string 20, total sint)", Db);
        _engine.Execute("create table order_items (order_id string 36, plant_id string 36, quantity sint, unit_price sint)", Db);

        // Customers (5) — IDs are 1-5
        _engine.Execute("upsert customers { name: 'Anna Müller', email: 'anna@test.de', city: 'München', joined_at: '2024-06-15' }", Db);        // id=1
        _engine.Execute("upsert customers { name: 'Thomas Wagner', email: 'thomas@test.de', city: 'Berlin', joined_at: '2024-07-20' }", Db);     // id=2
        _engine.Execute("upsert customers { name: 'Sophie Becker', email: 'sophie@test.de', city: 'Hamburg', joined_at: '2024-08-01' }", Db);     // id=3
        _engine.Execute("upsert customers { name: 'Markus Hoffmann', email: 'markus@test.de', city: 'Berlin', joined_at: '2024-11-10' }", Db);    // id=4
        _engine.Execute("upsert customers { name: 'Laura Fischer', email: 'laura@test.de', city: 'München', joined_at: '2025-01-05' }", Db);      // id=5

        // Orders (5) — IDs are 1-5
        _engine.Execute("upsert orders { customer_id: '1', order_date: '2024-12-01', status: 'delivered', total: 80 }", Db);    // id=1 Anna, delivered, >50 ✓
        _engine.Execute("upsert orders { customer_id: '1', order_date: '2025-01-15', status: 'pending', total: 30 }", Db);      // id=2 Anna, pending ✗
        _engine.Execute("upsert orders { customer_id: '2', order_date: '2024-11-20', status: 'delivered', total: 120 }", Db);   // id=3 Thomas, delivered, >50 ✓
        _engine.Execute("upsert orders { customer_id: '2', order_date: '2024-12-10', status: 'delivered', total: 40 }", Db);    // id=4 Thomas, delivered, ≤50 ✗
        _engine.Execute("upsert orders { customer_id: '4', order_date: '2025-02-01', status: 'delivered', total: 60 }", Db);    // id=5 Markus, delivered, >50 ✓

        // Order items — IDs are 1-3
        _engine.Execute("upsert order_items { order_id: '1', plant_id: '1', quantity: 3, unit_price: 10 }", Db);  // id=1 for order 1
        _engine.Execute("upsert order_items { order_id: '1', plant_id: '2', quantity: 5, unit_price: 10 }", Db);  // id=2 for order 1
        _engine.Execute("upsert order_items { order_id: '3', plant_id: '1', quantity: 2, unit_price: 20 }", Db);  // id=3 for order 3
        _engine.Execute("upsert order_items { order_id: '5', plant_id: '3', quantity: 1, unit_price: 15 }", Db);  // id=4 for order 5
    }

    public void Dispose()
    {
        _engine.Dispose();
        if (Directory.Exists(_tempDir))
            Directory.Delete(_tempDir, true);
    }

    private SproutResponse Run(string query) => _engine.Execute(query, Db);

    private void AssertSuccess(SproutResponse result, string context)
    {
        Assert.True(result.Operation != SproutOperation.Error,
            $"Query failed ({context}):\n{string.Join("\n", result.Errors?.Select(e => $"  {e.Code}: {e.Message}") ?? [])}");
    }

    // ── WHERE precedence: AND binds tighter than OR (SQL-conformant) ──
    // city = 'München' OR (city = 'Berlin' AND joined_at > '2024-09-01')
    // Matches: Anna (München), Markus (Berlin+Nov), Laura (München)
    // NOT Thomas (Berlin+Jul < Sep), NOT Sophie (Hamburg)

    [Fact]
    public void Where_Or_And_Precedence()
    {
        var r = Run("get customers where city = 'München' or city = 'Berlin' and joined_at > '2024-09-01'");
        AssertSuccess(r, "where or/and precedence");
        Assert.Equal(3, r.Affected);
        var names = r.Data?.Select(row => (string?)row["name"]).OrderBy(n => n).ToList();
        Assert.Equal(new[] { "Anna Müller", "Laura Fischer", "Markus Hoffmann" }, names);
    }

    // ── Step-by-step: simple get ──

    [Fact]
    public void Get_Customers()
    {
        var r = Run("get customers");
        AssertSuccess(r, "get customers");
        Assert.Equal(5, r.Affected);
    }

    [Fact]
    public void Get_Customers_Select()
    {
        var r = Run("get customers select name, email, city");
        AssertSuccess(r, "select");
        Assert.Equal(5, r.Affected);
        // _id should not be in result
        Assert.False(r.Data?[0].ContainsKey("_id"));
    }

    [Fact]
    public void Get_Customers_Where_Single()
    {
        var r = Run("get customers where city = 'München'");
        AssertSuccess(r, "where single");
        Assert.Equal(2, r.Affected);
    }

    [Fact]
    public void Get_Customers_Where_Or()
    {
        var r = Run("get customers where city = 'München' or city = 'Berlin'");
        AssertSuccess(r, "where or");
        Assert.Equal(4, r.Affected);
    }

    [Fact]
    public void Get_Customers_Where_OrderBy()
    {
        var r = Run("get customers where city = 'München' order by name asc");
        AssertSuccess(r, "where + order by");
        Assert.Equal(2, r.Affected);
        Assert.Equal("Anna Müller", (string?)r.Data?[0]["name"]);
        Assert.Equal("Laura Fischer", (string?)r.Data?[1]["name"]);
    }

    // ── Follow: single level ──

    [Fact]
    public void Follow_AllOrders_Flat()
    {
        var r = Run("get customers where name = 'Anna Müller' follow customers._id -> orders.customer_id as orders");
        AssertSuccess(r, "follow all orders");
        // Anna has 2 orders → 2 flat rows
        Assert.Equal(2, r.Affected);
        Assert.All(r.Data ?? [], row => Assert.Equal("Anna Müller", (string?)row["name"]));
    }

    [Fact]
    public void Follow_FilteredOrders()
    {
        var r = Run("get customers where name = 'Anna Müller' follow customers._id -> orders.customer_id as orders where status = 'delivered' and total > 50");
        AssertSuccess(r, "follow filtered orders");
        // Anna: only order 1 matches (delivered, total=80)
        Assert.Equal(1, r.Affected);
        var row = r.Data?[0];
        Assert.Equal("Anna Müller", (string?)row?["name"]);
        Assert.Equal("delivered", (string?)row?["orders.status"]);
        Assert.Equal(80, row?["orders.total"]);
    }

    [Fact]
    public void Follow_NoMatchingOrders_Dropped()
    {
        var r = Run("get customers where name = 'Laura Fischer' follow customers._id -> orders.customer_id as orders");
        AssertSuccess(r, "follow no orders");
        // INNER JOIN: Laura has no orders → dropped entirely
        Assert.Equal(0, r.Affected);
        Assert.Empty(r.Data ?? []);
    }

    // ── Follow: chained (orders → order_items) ──

    [Fact]
    public void Follow_Chained_OrdersAndItems()
    {
        var r = Run("get customers where name = 'Anna Müller' follow customers._id -> orders.customer_id as orders where status = 'delivered' and total > 50 follow orders._id -> order_items.order_id as items");
        AssertSuccess(r, "chained follow");
        // Anna → order 1 (delivered, 80) → 2 items → 2 flat rows
        Assert.Equal(2, r.Affected);
        Assert.All(r.Data ?? [], row =>
        {
            Assert.Equal("Anna Müller", (string?)row["name"]);
            Assert.Equal("delivered", (string?)row["orders.status"]);
            Assert.NotNull(row["items._id"]);
        });
        // Verify item quantities
        var quantities = r.Data?.Select(row => (int?)row["items.quantity"]).OrderBy(q => q).ToList();
        Assert.Equal(new int?[] { 3, 5 }, quantities);
    }

    [Fact]
    public void Follow_Chained_NoItems_NullColumns()
    {
        // Markus (id=4) has order 5 (delivered, 60 > 50) with 1 item
        var r = Run("get customers where name = 'Markus Hoffmann' follow customers._id -> orders.customer_id as orders where status = 'delivered' and total > 50 follow orders._id -> order_items.order_id as items");
        AssertSuccess(r, "chained follow markus");
        Assert.Equal(1, r.Affected);
        var row = r.Data?[0];
        Assert.Equal("Markus Hoffmann", (string?)row?["name"]);
        Assert.Equal(60, row?["orders.total"]);
        Assert.Equal(1, row?["items.quantity"]);
    }

    // ── The FULL complex query with verified data ──

    [Fact]
    public void FullQuery_Select_Where_OrderBy_Limit_TwoFollows()
    {
        // WHERE: city='München' OR (city='Berlin' AND joined_at > '2024-09-01')
        // Matches: Anna (München), Markus (Berlin, Nov), Laura (München) — sorted by name asc
        //
        // Follow 1 (INNER): orders WHERE status='delivered' AND total > 50
        //   Anna  (id=1) → order 1 (delivered, 80) ✓
        //   Laura (id=5) → no orders → DROPPED
        //   Markus(id=4) → order 5 (delivered, 60) ✓
        //
        // Follow 2 (INNER): orders._id -> order_items.order_id
        //   Anna/order 1 → items 1,2 (qty 3 + 5) → 2 rows
        //   Markus/order 5 → item 4 (qty 1) → 1 row
        //
        // Total: 3 flat rows, ordered by name asc

        var query = "get customers select name, email, city "
                  + "where city = 'München' or city = 'Berlin' and joined_at > '2024-09-01' "
                  + "order by name asc limit 20 "
                  + "follow customers._id -> orders.customer_id as orders where status = 'delivered' and total > 50 "
                  + "follow orders._id -> order_items.order_id as items";

        var r = Run(query);
        AssertSuccess(r, "full complex query");
        Assert.NotNull(r.Data);
        Assert.Equal(3, r.Data.Count);

        // Row 0+1: Anna Müller (2 items from order 1)
        Assert.Equal("Anna Müller", (string?)r.Data[0]["name"]);
        Assert.Equal("Anna Müller", (string?)r.Data[1]["name"]);
        Assert.Equal(80, r.Data[0]["orders.total"]);
        Assert.NotNull(r.Data[0]["items._id"]);
        Assert.NotNull(r.Data[1]["items._id"]);

        // Row 2: Markus Hoffmann (order 5, 1 item)
        Assert.Equal("Markus Hoffmann", (string?)r.Data[2]["name"]);
        Assert.Equal(60, r.Data[2]["orders.total"]);
        Assert.NotNull(r.Data[2]["items._id"]);
        Assert.Equal(1, r.Data[2]["items.quantity"]);

        // Select columns: name, email, city — no _id, no joined_at
        Assert.False(r.Data[0].ContainsKey("_id"));
        Assert.False(r.Data[0].ContainsKey("joined_at"));
        Assert.True(r.Data[0].ContainsKey("name"));
        Assert.True(r.Data[0].ContainsKey("email"));
        Assert.True(r.Data[0].ContainsKey("city"));

        // Follow columns should be present
        Assert.True(r.Data[0].ContainsKey("orders._id"));
        Assert.True(r.Data[0].ContainsKey("orders.status"));
        Assert.True(r.Data[0].ContainsKey("items.order_id"));
        Assert.True(r.Data[0].ContainsKey("items.quantity"));
    }

    // ── Same query without select (all columns) ──

    [Fact]
    public void FullQuery_NoSelect_TwoFollows()
    {
        var query = "get customers "
                  + "where city = 'München' or city = 'Berlin' and joined_at > '2024-09-01' "
                  + "order by name asc limit 20 "
                  + "follow customers._id -> orders.customer_id as orders where status = 'delivered' and total > 50 "
                  + "follow orders._id -> order_items.order_id as items";

        var r = Run(query);
        AssertSuccess(r, "full query no select");
        // INNER JOIN: Laura dropped (no orders) → 3 rows
        Assert.Equal(3, r.Data?.Count);
        // Without select, _id should be present
        Assert.True(r.Data?[0].ContainsKey("_id"));
    }

    // ── Single follow without chaining ──

    [Fact]
    public void FullQuery_SingleFollow_MultipleCustomers()
    {
        var query = "get customers select name, city "
                  + "where city = 'München' or city = 'Berlin' and joined_at > '2024-09-01' "
                  + "order by name asc "
                  + "follow customers._id -> orders.customer_id as orders";

        var r = Run(query);
        AssertSuccess(r, "single follow multi customers");
        // INNER JOIN: Anna (2 orders) + Markus (1 order) = 3 rows. Laura dropped (no orders).
        Assert.Equal(3, r.Data?.Count);
    }

    // ── Follow-level SELECT: only selected columns from each follow ──

    [Fact]
    public void FollowSelect_OnlySelectedColumnsInResult()
    {
        var query = "get customers select name, city "
                  + "where city = 'München' or city = 'Berlin' and joined_at > '2024-09-01' "
                  + "order by name asc "
                  + "follow customers._id -> orders.customer_id as orders select status, total "
                  + "where status = 'delivered' and total > 30 "
                  + "follow orders._id -> order_items.order_id as items select quantity, unit_price";

        var r = Run(query);
        AssertSuccess(r, "follow-level select");
        Assert.NotNull(r.Data);
        Assert.True(r.Data.Count > 0, "should have results");

        var first = r.Data[0];

        // Main select: name, city — no _id, no email, no joined_at
        Assert.True(first.ContainsKey("name"));
        Assert.True(first.ContainsKey("city"));
        Assert.False(first.ContainsKey("_id"));
        Assert.False(first.ContainsKey("email"));
        Assert.False(first.ContainsKey("joined_at"));

        // Follow 1 select: status, total — no customer_id, no order_date, no orders._id
        Assert.True(first.ContainsKey("orders.status"));
        Assert.True(first.ContainsKey("orders.total"));
        Assert.False(first.ContainsKey("orders.customer_id"));
        Assert.False(first.ContainsKey("orders.order_date"));
        Assert.False(first.ContainsKey("orders._id"));

        // Follow 2 select: quantity, unit_price — no order_id, no plant_id, no items._id
        Assert.True(first.ContainsKey("items.quantity"));
        Assert.True(first.ContainsKey("items.unit_price"));
        Assert.False(first.ContainsKey("items.order_id"));
        Assert.False(first.ContainsKey("items.plant_id"));
        Assert.False(first.ContainsKey("items._id"));
    }

    [Fact]
    public void FollowSelect_CorrectValues()
    {
        // Anna (München) → order 1 (delivered, 80 > 30) → 2 items
        // Markus (Berlin, Nov) → order 5 (delivered, 60 > 30) → 1 item
        // Laura (München) → no orders → DROPPED
        // Total: 3 flat rows

        var query = "get customers select name, city "
                  + "where city = 'München' or city = 'Berlin' and joined_at > '2024-09-01' "
                  + "order by name asc "
                  + "follow customers._id -> orders.customer_id as orders select status, total "
                  + "where status = 'delivered' and total > 30 "
                  + "follow orders._id -> order_items.order_id as items select quantity, unit_price";

        var r = Run(query);
        AssertSuccess(r, "follow-select values");
        Assert.Equal(3, r.Data?.Count);

        // Anna rows (2 items)
        Assert.Equal("Anna Müller", (string?)r.Data?[0]["name"]);
        Assert.Equal("delivered", (string?)r.Data?[0]["orders.status"]);
        Assert.Equal(80, r.Data?[0]["orders.total"]);

        // Markus row (1 item)
        Assert.Equal("Markus Hoffmann", (string?)r.Data?[2]["name"]);
        Assert.Equal(60, r.Data?[2]["orders.total"]);
        Assert.Equal(1, r.Data?[2]["items.quantity"]);
    }

    // ── Paging with follow ──

    [Fact]
    public void Paging_WithFollow_HasNextQuery()
    {
        // Use page 1 size 2 to force paging (3 results, page size 2)
        var query = "get customers select name, city "
                  + "where city = 'München' or city = 'Berlin' and joined_at > '2024-09-01' "
                  + "order by name asc "
                  + "page 1 size 2 "
                  + "follow customers._id -> orders.customer_id as orders select status, total "
                  + "where status = 'delivered' and total > 30 "
                  + "follow orders._id -> order_items.order_id as items select quantity, unit_price";

        var r = Run(query);
        AssertSuccess(r, "paging with follow page 1");
        Assert.NotNull(r.Paging);
        Assert.Equal(1, r.Paging.Page);
        Assert.Equal(2, r.Data?.Count);
        Assert.NotNull(r.Paging.Next);

        // Next query must still contain follow clauses
        Assert.Contains("follow", r.Paging.Next, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("page 2", r.Paging.Next, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void Paging_WithFollow_Page2_HasRemainingRow()
    {
        // Page 2 should have 1 row (Markus)
        var query = "get customers select name, city "
                  + "where city = 'München' or city = 'Berlin' and joined_at > '2024-09-01' "
                  + "order by name asc "
                  + "page 2 size 2 "
                  + "follow customers._id -> orders.customer_id as orders select status, total "
                  + "where status = 'delivered' and total > 30 "
                  + "follow orders._id -> order_items.order_id as items select quantity, unit_price";

        var r = Run(query);
        AssertSuccess(r, "paging with follow page 2");
        Assert.NotNull(r.Paging);
        Assert.Equal(2, r.Paging.Page);
        Assert.Equal(1, r.Data?.Count);
        Assert.Equal("Markus Hoffmann", (string?)r.Data?[0]["name"]);
    }

    [Fact]
    public void Paging_WithFollow_FollowColumnsOnEveryPage()
    {
        var query = "get customers select name, city "
                  + "where city = 'München' or city = 'Berlin' and joined_at > '2024-09-01' "
                  + "order by name asc "
                  + "page 1 size 2 "
                  + "follow customers._id -> orders.customer_id as orders select status, total "
                  + "where status = 'delivered' and total > 30 "
                  + "follow orders._id -> order_items.order_id as items select quantity, unit_price";

        var r = Run(query);
        AssertSuccess(r, "follow columns on page 1");

        // Follow columns must be present even on paged results
        var first = r.Data?[0];
        Assert.NotNull(first);
        Assert.True(first.ContainsKey("orders.status"), "orders.status missing on page 1");
        Assert.True(first.ContainsKey("orders.total"), "orders.total missing on page 1");
        Assert.True(first.ContainsKey("items.quantity"), "items.quantity missing on page 1");
        Assert.True(first.ContainsKey("items.unit_price"), "items.unit_price missing on page 1");
    }

    // ── Paging with formatted (multi-line) query ──

    [Fact]
    public void Paging_FormattedQuery_NextPagePreservesFollow()
    {
        // Simulate a formatted query with newlines (as the query editor would send)
        var query = "get customers select name, city\n"
                  + "    where city = 'München' or city = 'Berlin' and joined_at > '2024-09-01'\n"
                  + "    order by name asc\n"
                  + "    page 1 size 2\n"
                  + "    follow customers._id -> orders.customer_id as orders\n"
                  + "        select status, total\n"
                  + "        where status = 'delivered' and total > 30\n"
                  + "    follow orders._id -> order_items.order_id as items\n"
                  + "        select quantity, unit_price";

        var r = Run(query);
        AssertSuccess(r, "formatted paging page 1");
        Assert.NotNull(r.Paging);
        Assert.Equal(2, r.Data?.Count);
        Assert.NotNull(r.Paging.Next);

        // Next page query must contain follow and page 2
        Assert.Contains("follow", r.Paging.Next, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("page 2", r.Paging.Next, StringComparison.OrdinalIgnoreCase);

        // Execute next page query
        var r2 = Run(r.Paging.Next);
        AssertSuccess(r2, "formatted paging page 2");
        Assert.Equal(1, r2.Data?.Count);
        Assert.Equal("Markus Hoffmann", (string?)r2.Data?[0]["name"]);

        // Follow columns must still be present on page 2
        Assert.True(r2.Data?[0].ContainsKey("orders.status"), "orders.status missing on page 2");
        Assert.True(r2.Data?[0].ContainsKey("items.quantity"), "items.quantity missing on page 2");
    }

    // ── Edge: Thomas NOT in result (Berlin but joined too early) ──

    [Fact]
    public void Thomas_Excluded_By_And_Precedence()
    {
        var r = Run("get customers select name where city = 'München' or city = 'Berlin' and joined_at > '2024-09-01'");
        AssertSuccess(r, "thomas excluded");
        var names = r.Data?.Select(row => (string?)row["name"]).ToList();
        Assert.DoesNotContain("Thomas Wagner", names);
        Assert.Contains("Markus Hoffmann", names);
    }
}

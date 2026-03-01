namespace SproutDB.Core.Tests;

public class FollowTests : IDisposable
{
    private readonly string _tempDir;
    private readonly SproutEngine _engine;

    public FollowTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), $"sproutdb-test-{Guid.NewGuid()}");
        _engine = new SproutEngine(_tempDir);
        _engine.Execute("create database", "testdb");

        // Users table
        _engine.Execute(
            "create table users (name string 100, email string 200, active bool)",
            "testdb");
        _engine.Execute("upsert users {name: 'Alice', email: 'alice@test.com', active: true}", "testdb");
        _engine.Execute("upsert users {name: 'Bob', email: 'bob@test.com', active: true}", "testdb");
        _engine.Execute("upsert users {name: 'Charlie', email: 'charlie@test.com', active: false}", "testdb");

        // Orders table with user_id foreign key
        _engine.Execute(
            "create table orders (user_id ulong, product string 100, amount double, status string 50)",
            "testdb");
        _engine.Execute("upsert orders {user_id: 1, product: 'Widget', amount: 25.50, status: 'completed'}", "testdb");
        _engine.Execute("upsert orders {user_id: 1, product: 'Gadget', amount: 75.00, status: 'pending'}", "testdb");
        _engine.Execute("upsert orders {user_id: 2, product: 'Doohickey', amount: 12.25, status: 'completed'}", "testdb");
        _engine.Execute("upsert orders {user_id: 3, product: 'Thingamajig', amount: 50.00, status: 'completed'}", "testdb");

        // Products table for chained joins
        _engine.Execute(
            "create table products (name string 100, price double)",
            "testdb");
        _engine.Execute("upsert products {name: 'Widget', price: 9.99}", "testdb");
        _engine.Execute("upsert products {name: 'Gadget', price: 19.99}", "testdb");
        _engine.Execute("upsert products {name: 'Doohickey', price: 4.99}", "testdb");
        _engine.Execute("upsert products {name: 'Thingamajig', price: 14.99}", "testdb");
    }

    public void Dispose()
    {
        _engine.Dispose();
        if (Directory.Exists(_tempDir))
            Directory.Delete(_tempDir, true);
    }

    // ── Basic follow (#072) ─────────────────────────────────

    [Fact]
    public void Follow_Basic_JoinById()
    {
        var r = _engine.Execute(
            "get users follow users._id -> orders.user_id as orders",
            "testdb");

        Assert.Equal(SproutOperation.Get, r.Operation);
        Assert.NotNull(r.Data);
        Assert.Equal(3, r.Data.Count);

        // Alice (id=1) has 2 orders
        var alice = r.Data.First(row => (string?)row["name"] == "Alice");
        var aliceOrders = (IList<Dictionary<string, object?>>)alice["orders"]!;
        Assert.Equal(2, aliceOrders.Count);

        // Bob (id=2) has 1 order
        var bob = r.Data.First(row => (string?)row["name"] == "Bob");
        var bobOrders = (IList<Dictionary<string, object?>>)bob["orders"]!;
        Assert.Single(bobOrders);

        // Charlie (id=3) has 1 order
        var charlie = r.Data.First(row => (string?)row["name"] == "Charlie");
        var charlieOrders = (IList<Dictionary<string, object?>>)charlie["orders"]!;
        Assert.Single(charlieOrders);
    }

    [Fact]
    public void Follow_NoMatchingRows_EmptyArray()
    {
        // Add user with no orders
        _engine.Execute("upsert users {name: 'Diana', email: 'diana@test.com', active: true}", "testdb");

        var r = _engine.Execute(
            "get users where name = 'Diana' follow users._id -> orders.user_id as orders",
            "testdb");

        Assert.NotNull(r.Data);
        Assert.Single(r.Data);
        var diana = r.Data[0];
        var orders = (IList<Dictionary<string, object?>>)diana["orders"]!;
        Assert.Empty(orders);
    }

    [Fact]
    public void Follow_NestedRowsContainAllColumns()
    {
        var r = _engine.Execute(
            "get users where name = 'Bob' follow users._id -> orders.user_id as orders",
            "testdb");

        var bob = r.Data![0];
        var orders = (IList<Dictionary<string, object?>>)bob["orders"]!;
        var order = orders[0];

        Assert.True(order.ContainsKey("_id"));
        Assert.True(order.ContainsKey("user_id"));
        Assert.True(order.ContainsKey("product"));
        Assert.True(order.ContainsKey("amount"));
        Assert.True(order.ContainsKey("status"));
    }

    [Fact]
    public void Follow_WithMainWhere()
    {
        var r = _engine.Execute(
            "get users where active = true follow users._id -> orders.user_id as orders",
            "testdb");

        Assert.NotNull(r.Data);
        // Only active users: Alice and Bob
        Assert.Equal(2, r.Data.Count);
        Assert.True(r.Data.All(row => (string?)row["name"] is "Alice" or "Bob"));
    }

    // ── Filtered follow (#073) ──────────────────────────────

    [Fact]
    public void Follow_WithFollowWhere()
    {
        var r = _engine.Execute(
            "get users follow users._id -> orders.user_id as orders where status = 'completed'",
            "testdb");

        Assert.NotNull(r.Data);

        // Alice: 2 orders, but only 1 completed
        var alice = r.Data.First(row => (string?)row["name"] == "Alice");
        var aliceOrders = (IList<Dictionary<string, object?>>)alice["orders"]!;
        Assert.Single(aliceOrders);
        Assert.Equal("completed", (string?)aliceOrders[0]["status"]);
    }

    [Fact]
    public void Follow_WithBothWheres()
    {
        var r = _engine.Execute(
            "get users where active = true follow users._id -> orders.user_id as orders where status = 'completed'",
            "testdb");

        Assert.NotNull(r.Data);
        Assert.Equal(2, r.Data.Count); // Alice and Bob

        var alice = r.Data.First(row => (string?)row["name"] == "Alice");
        var aliceOrders = (IList<Dictionary<string, object?>>)alice["orders"]!;
        Assert.Single(aliceOrders); // Only completed

        var bob = r.Data.First(row => (string?)row["name"] == "Bob");
        var bobOrders = (IList<Dictionary<string, object?>>)bob["orders"]!;
        Assert.Single(bobOrders); // Bob's order is completed
    }

    [Fact]
    public void Follow_WhereFiltersAllTarget_EmptyArray()
    {
        var r = _engine.Execute(
            "get users where name = 'Alice' follow users._id -> orders.user_id as orders where status = 'cancelled'",
            "testdb");

        var alice = r.Data![0];
        var orders = (IList<Dictionary<string, object?>>)alice["orders"]!;
        Assert.Empty(orders);
    }

    // ── Multiple follows (#074) ─────────────────────────────

    [Fact]
    public void Follow_Multiple_ChainedJoins()
    {
        // Add product_id to orders referencing products by name
        // We need a numeric link. Let's use a dedicated table for this test.
        _engine.Execute(
            "create table items (code uint, label string 50)",
            "testdb");
        _engine.Execute("upsert items {code: 100, label: 'Alpha'}", "testdb");
        _engine.Execute("upsert items {code: 200, label: 'Beta'}", "testdb");

        _engine.Execute(
            "create table shipments (item_code uint, destination string 50, order_ref ulong)",
            "testdb");
        _engine.Execute("upsert shipments {item_code: 100, destination: 'Berlin', order_ref: 1}", "testdb");
        _engine.Execute("upsert shipments {item_code: 100, destination: 'Munich', order_ref: 2}", "testdb");
        _engine.Execute("upsert shipments {item_code: 200, destination: 'Hamburg', order_ref: 3}", "testdb");

        var r = _engine.Execute(
            "get items follow items.code -> shipments.item_code as shipments follow items._id -> orders.user_id as user_orders",
            "testdb");

        Assert.NotNull(r.Data);
        Assert.Equal(2, r.Data.Count);

        // Item 1 (code=100): 2 shipments
        var item1 = r.Data.First(row => (string?)row["label"] == "Alpha");
        var shipments = (IList<Dictionary<string, object?>>)item1["shipments"]!;
        Assert.Equal(2, shipments.Count);

        // Item 1 (id=1): user_orders where user_id=1
        var userOrders = (IList<Dictionary<string, object?>>)item1["user_orders"]!;
        Assert.Equal(2, userOrders.Count); // Alice has id=1 → 2 orders with user_id=1
    }

    [Fact]
    public void Follow_Multiple_EachWithWhere()
    {
        _engine.Execute(
            "create table tags (order_id ulong, tag string 50)",
            "testdb");
        _engine.Execute("upsert tags {order_id: 1, tag: 'urgent'}", "testdb");
        _engine.Execute("upsert tags {order_id: 1, tag: 'fragile'}", "testdb");
        _engine.Execute("upsert tags {order_id: 2, tag: 'standard'}", "testdb");
        _engine.Execute("upsert tags {order_id: 3, tag: 'urgent'}", "testdb");

        var r = _engine.Execute(
            "get users where name = 'Alice' follow users._id -> orders.user_id as orders where status = 'completed' follow users._id -> tags.order_id as tags where tag = 'urgent'",
            "testdb");

        Assert.NotNull(r.Data);
        Assert.Single(r.Data);
        var alice = r.Data[0];

        // Only completed orders for Alice
        var orders = (IList<Dictionary<string, object?>>)alice["orders"]!;
        Assert.Single(orders);

        // Only urgent tags where order_id = 1 (Alice's id)
        var tags = (IList<Dictionary<string, object?>>)alice["tags"]!;
        Assert.Single(tags);
        Assert.Equal("urgent", (string?)tags[0]["tag"]);
    }

    // ── Error cases ─────────────────────────────────────────

    [Fact]
    public void Follow_UnknownTargetTable_Error()
    {
        var r = _engine.Execute(
            "get users follow users._id -> nonexistent.user_id as stuff",
            "testdb");

        Assert.Equal(SproutOperation.Error, r.Operation);
        Assert.NotNull(r.Errors);
        Assert.Equal("UNKNOWN_TABLE", r.Errors[0].Code);
    }

    [Fact]
    public void Follow_UnknownTargetColumn_Error()
    {
        var r = _engine.Execute(
            "get users follow users._id -> orders.nonexistent as orders",
            "testdb");

        Assert.Equal(SproutOperation.Error, r.Operation);
        Assert.NotNull(r.Errors);
        Assert.Equal("UNKNOWN_COLUMN", r.Errors[0].Code);
    }

    [Fact]
    public void Follow_UnknownSourceColumn_Error()
    {
        var r = _engine.Execute(
            "get users follow users.nonexistent -> orders.user_id as orders",
            "testdb");

        Assert.Equal(SproutOperation.Error, r.Operation);
        Assert.NotNull(r.Errors);
        Assert.Equal("UNKNOWN_COLUMN", r.Errors[0].Code);
    }

    [Fact]
    public void Follow_UnknownWhereColumn_Error()
    {
        var r = _engine.Execute(
            "get users follow users._id -> orders.user_id as orders where nonexistent = 'x'",
            "testdb");

        Assert.Equal(SproutOperation.Error, r.Operation);
        Assert.NotNull(r.Errors);
        Assert.Equal("UNKNOWN_COLUMN", r.Errors[0].Code);
    }

    // ── With select ─────────────────────────────────────────

    [Fact]
    public void Follow_WithSelect()
    {
        var r = _engine.Execute(
            "get users select name follow users._id -> orders.user_id as orders",
            "testdb");

        Assert.NotNull(r.Data);
        var alice = r.Data.First(row => (string?)row["name"] == "Alice");
        // Should have name + orders (no email, no active, no id since select only name)
        Assert.False(alice.ContainsKey("email"));
        Assert.True(alice.ContainsKey("orders"));
    }

    // ── With order by / limit ───────────────────────────────

    [Fact]
    public void Follow_WithOrderByAndLimit()
    {
        var r = _engine.Execute(
            "get users order by name limit 2 follow users._id -> orders.user_id as orders",
            "testdb");

        Assert.NotNull(r.Data);
        Assert.Equal(2, r.Data.Count);
        // First two alphabetically: Alice, Bob
        Assert.Equal("Alice", (string?)r.Data[0]["name"]);
        Assert.Equal("Bob", (string?)r.Data[1]["name"]);
        Assert.True(r.Data[0].ContainsKey("orders"));
    }
}

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

    // ── Basic follow — flat INNER JOIN ────────────────────────

    [Fact]
    public void Follow_Basic_JoinById()
    {
        var r = _engine.Execute(
            "get users follow users._id -> orders.user_id as orders",
            "testdb");

        Assert.Equal(SproutOperation.Get, r.Operation);
        Assert.NotNull(r.Data);

        // Alice has 2 orders, Bob has 1, Charlie has 1 → 4 flat rows
        Assert.Equal(4, r.Data.Count);

        // Alice rows
        var aliceRows = r.Data.Where(row => (string?)row["name"] == "Alice").ToList();
        Assert.Equal(2, aliceRows.Count);
        Assert.Contains(aliceRows, row => (string?)row["orders.product"] == "Widget");
        Assert.Contains(aliceRows, row => (string?)row["orders.product"] == "Gadget");

        // Bob rows
        var bobRows = r.Data.Where(row => (string?)row["name"] == "Bob").ToList();
        Assert.Single(bobRows);
        Assert.Equal("Doohickey", (string?)bobRows[0]["orders.product"]);

        // Charlie rows
        var charlieRows = r.Data.Where(row => (string?)row["name"] == "Charlie").ToList();
        Assert.Single(charlieRows);
    }

    [Fact]
    public void Follow_NoMatchingRows_Dropped()
    {
        // Add user with no orders
        _engine.Execute("upsert users {name: 'Diana', email: 'diana@test.com', active: true}", "testdb");

        var r = _engine.Execute(
            "get users where name = 'Diana' follow users._id -> orders.user_id as orders",
            "testdb");

        Assert.NotNull(r.Data);
        // INNER JOIN: Diana has no orders → dropped entirely
        Assert.Empty(r.Data);
    }

    [Fact]
    public void Follow_FlatRowsContainAllColumns()
    {
        var r = _engine.Execute(
            "get users where name = 'Bob' follow users._id -> orders.user_id as orders",
            "testdb");

        Assert.NotNull(r.Data);
        Assert.Single(r.Data);
        var row = r.Data[0];

        // Parent columns
        Assert.True(row.ContainsKey("_id"));
        Assert.True(row.ContainsKey("name"));
        Assert.True(row.ContainsKey("email"));

        // Child columns (prefixed with alias)
        Assert.True(row.ContainsKey("orders._id"));
        Assert.True(row.ContainsKey("orders.user_id"));
        Assert.True(row.ContainsKey("orders.product"));
        Assert.True(row.ContainsKey("orders.amount"));
        Assert.True(row.ContainsKey("orders.status"));
    }

    [Fact]
    public void Follow_WithMainWhere()
    {
        var r = _engine.Execute(
            "get users where active = true follow users._id -> orders.user_id as orders",
            "testdb");

        Assert.NotNull(r.Data);
        // Active users: Alice (2 orders) + Bob (1 order) = 3 flat rows
        Assert.Equal(3, r.Data.Count);
        Assert.True(r.Data.All(row => (string?)row["name"] is "Alice" or "Bob"));
    }

    // ── Filtered follow ──────────────────────────────────────

    [Fact]
    public void Follow_WithFollowWhere()
    {
        var r = _engine.Execute(
            "get users follow users._id -> orders.user_id as orders where status = 'completed'",
            "testdb");

        Assert.NotNull(r.Data);

        // Alice: 1 completed, Bob: 1 completed, Charlie: 1 completed = 3 rows
        var aliceRows = r.Data.Where(row => (string?)row["name"] == "Alice").ToList();
        Assert.Single(aliceRows);
        Assert.Equal("completed", (string?)aliceRows[0]["orders.status"]);
    }

    [Fact]
    public void Follow_WithBothWheres()
    {
        var r = _engine.Execute(
            "get users where active = true follow users._id -> orders.user_id as orders where status = 'completed'",
            "testdb");

        Assert.NotNull(r.Data);
        // Active users: Alice (1 completed) + Bob (1 completed) = 2 flat rows
        Assert.Equal(2, r.Data.Count);

        var aliceRow = r.Data.First(row => (string?)row["name"] == "Alice");
        Assert.Equal("completed", (string?)aliceRow["orders.status"]);

        var bobRow = r.Data.First(row => (string?)row["name"] == "Bob");
        Assert.Equal("completed", (string?)bobRow["orders.status"]);
    }

    [Fact]
    public void Follow_WhereFiltersAllTarget_Dropped()
    {
        var r = _engine.Execute(
            "get users where name = 'Alice' follow users._id -> orders.user_id as orders where status = 'cancelled'",
            "testdb");

        Assert.NotNull(r.Data);
        // INNER JOIN: no matching orders → Alice dropped
        Assert.Empty(r.Data);
    }

    // ── Multiple follows ─────────────────────────────────────

    [Fact]
    public void Follow_Multiple_ChainedJoins()
    {
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
        // Item 1 (Alpha, code=100): 2 shipments × user_orders for id=1 (Alice: 2 orders) = 4 rows
        // Item 2 (Beta, code=200): 1 shipment × user_orders for id=2 (Bob: 1 order) = 1 row
        // Total: 5 rows (cartesian product of both follows per parent)
        var alphaRows = r.Data.Where(row => (string?)row["label"] == "Alpha").ToList();
        Assert.Equal(4, alphaRows.Count); // 2 shipments × 2 user_orders

        var betaRows = r.Data.Where(row => (string?)row["label"] == "Beta").ToList();
        Assert.Single(betaRows); // 1 shipment × 1 user_order
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
        // Alice: 1 completed order × 1 urgent tag = 1 flat row
        Assert.Single(r.Data);
        var row = r.Data[0];
        Assert.Equal("Alice", (string?)row["name"]);
        Assert.Equal("completed", (string?)row["orders.status"]);
        Assert.Equal("urgent", (string?)row["tags.tag"]);
    }

    // ── Chained follow (second follow joins on result of first) ──

    [Fact]
    public void Follow_Chained_SecondFollowUsesFirstResult()
    {
        // users -> orders -> tags (chained: second follow on orders._id)
        _engine.Execute(
            "create table tags (order_id ulong, tag string 50)",
            "testdb");
        _engine.Execute("upsert tags {order_id: 1, tag: 'urgent'}", "testdb");
        _engine.Execute("upsert tags {order_id: 1, tag: 'fragile'}", "testdb");
        _engine.Execute("upsert tags {order_id: 3, tag: 'priority'}", "testdb");

        var r = _engine.Execute(
            "get users where name = 'Alice' follow users._id -> orders.user_id as orders follow orders._id -> tags.order_id as tags",
            "testdb");

        Assert.NotNull(r.Data);
        // Alice has 2 orders (id=1: Widget, id=2: Gadget)
        // Order 1 has 2 tags (urgent, fragile) → 2 rows
        // Order 2 has 0 tags → INNER JOIN drops it
        // Total: 2 rows
        Assert.Equal(2, r.Data.Count);

        Assert.Contains(r.Data, row => (string?)row["tags.tag"] == "urgent");
        Assert.Contains(r.Data, row => (string?)row["tags.tag"] == "fragile");
        Assert.True(r.Data.All(row => (string?)row["orders.product"] == "Widget"));
    }

    [Fact]
    public void Follow_Chained_WithSelectWithoutId()
    {
        // The exact bug scenario: select without _id + chained follows
        _engine.Execute(
            "create table tags (order_id ulong, tag string 50)",
            "testdb");
        _engine.Execute("upsert tags {order_id: 1, tag: 'rush'}", "testdb");

        var r = _engine.Execute(
            "get users select name where name = 'Alice' follow users._id -> orders.user_id as orders follow orders._id -> tags.order_id as tags",
            "testdb");

        Assert.NotNull(r.Data);
        // Alice, 2 orders: order 1 has 1 tag → 1 row. Order 2 has 0 tags → dropped.
        Assert.Single(r.Data);
        Assert.False(r.Data[0].ContainsKey("_id")); // not in select
        Assert.True(r.Data[0].ContainsKey("orders.product"));
        Assert.Equal("rush", (string?)r.Data[0]["tags.tag"]);
    }

    // ── Join types (->?, ?->, ?->?) ─────────────────────────

    [Fact]
    public void Follow_LeftJoin_KeepsUnmatchedSource()
    {
        _engine.Execute("upsert users {name: 'Diana', email: 'diana@test.com', active: true}", "testdb");

        var r = _engine.Execute(
            "get users where name = 'Diana' or name = 'Alice' follow users._id ->? orders.user_id as orders",
            "testdb");

        Assert.NotNull(r.Data);
        // Alice: 2 orders = 2 rows, Diana: 0 orders = 1 row with null target
        var dianaRows = r.Data.Where(row => (string?)row["name"] == "Diana").ToList();
        Assert.Single(dianaRows);
        Assert.Null(dianaRows[0]["orders._id"]);
        Assert.Null(dianaRows[0]["orders.product"]);

        var aliceRows = r.Data.Where(row => (string?)row["name"] == "Alice").ToList();
        Assert.Equal(2, aliceRows.Count);
    }

    [Fact]
    public void Follow_RightJoin_KeepsUnmatchedTarget()
    {
        // Add an order with user_id that doesn't exist
        _engine.Execute("upsert orders {user_id: 999, product: 'Orphan', amount: 1.0, status: 'lost'}", "testdb");

        var r = _engine.Execute(
            "get users where name = 'Alice' follow users._id ?-> orders.user_id as orders",
            "testdb");

        Assert.NotNull(r.Data);
        // Alice matched orders + unmatched orders (user_id != Alice's id)
        var orphanRows = r.Data.Where(row => (string?)row["orders.product"] == "Orphan").ToList();
        Assert.Single(orphanRows);
        Assert.Null(orphanRows[0]["name"]); // source is null for unmatched target
    }

    [Fact]
    public void Follow_OuterJoin_KeepsBothUnmatched()
    {
        _engine.Execute("upsert users {name: 'Eve', email: 'eve@test.com', active: true}", "testdb");
        _engine.Execute("upsert orders {user_id: 888, product: 'Ghost', amount: 0.0, status: 'phantom'}", "testdb");

        var r = _engine.Execute(
            "get users where name = 'Eve' or name = 'Alice' follow users._id ?->? orders.user_id as orders",
            "testdb");

        Assert.NotNull(r.Data);

        // Eve: no orders → 1 row with null target
        var eveRows = r.Data.Where(row => (string?)row["name"] == "Eve").ToList();
        Assert.Single(eveRows);
        Assert.Null(eveRows[0]["orders._id"]);

        // Ghost order: no matching user → 1 row with null source
        var ghostRows = r.Data.Where(row => (string?)row["orders.product"] == "Ghost").ToList();
        Assert.Single(ghostRows);
        Assert.Null(ghostRows[0]["name"]);
    }

    [Fact]
    public void Follow_LeftJoin_WithFollowWhere_UnmatchedKept()
    {
        var r = _engine.Execute(
            "get users where name = 'Alice' follow users._id ->? orders.user_id as orders where status = 'cancelled'",
            "testdb");

        Assert.NotNull(r.Data);
        // Alice has orders but none cancelled → left join keeps Alice with nulls
        Assert.Single(r.Data);
        Assert.Equal("Alice", (string?)r.Data[0]["name"]);
        Assert.Null(r.Data[0]["orders._id"]);
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
        var aliceRow = r.Data.First(row => (string?)row["name"] == "Alice");
        // Should have name + orders columns (no email, no active)
        Assert.False(aliceRow.ContainsKey("email"));
        Assert.True(aliceRow.ContainsKey("orders.product"));
    }

    [Fact]
    public void Follow_WithSelect_WithoutId_StillJoins()
    {
        // select without _id — follow uses _id as source, must still work
        var r = _engine.Execute(
            "get users select name, email follow users._id -> orders.user_id as orders",
            "testdb");

        Assert.NotNull(r.Data);
        // Alice has 2 orders, Bob 1, Charlie 1 = 4 flat rows
        Assert.Equal(4, r.Data.Count);

        var aliceRows = r.Data.Where(row => (string?)row["name"] == "Alice").ToList();
        Assert.Equal(2, aliceRows.Count);

        // _id should NOT be in the output (not in select)
        Assert.False(aliceRows[0].ContainsKey("_id"));
        // But orders columns should be there
        Assert.True(aliceRows[0].ContainsKey("orders.product"));
    }

    [Fact]
    public void Follow_WithSelect_FullComplexQuery()
    {
        // The exact pattern from the bug: select without _id + where + order by + follow
        var r = _engine.Execute(
            "get users select name, email where active = true order by name asc follow users._id -> orders.user_id as orders where status = 'completed'",
            "testdb");

        Assert.NotNull(r.Data);
        // Active users: Alice (1 completed) + Bob (1 completed) = 2 flat rows
        Assert.Equal(2, r.Data.Count);
        Assert.Equal("Alice", (string?)r.Data[0]["name"]);
        Assert.Equal("Bob", (string?)r.Data[1]["name"]);
        Assert.Equal("completed", (string?)r.Data[0]["orders.status"]);
        Assert.False(r.Data[0].ContainsKey("_id")); // not in select
    }

    // ── With order by / limit ───────────────────────────────

    [Fact]
    public void Follow_WithOrderByAndLimit()
    {
        var r = _engine.Execute(
            "get users order by name limit 2 follow users._id -> orders.user_id as orders",
            "testdb");

        Assert.NotNull(r.Data);
        // Limit applies after flattening: 2 flat rows total
        Assert.Equal(2, r.Data.Count);
        Assert.True(r.Data.All(row => row.ContainsKey("orders.product")));
    }

    // ── Follow select (column projection) ───────────────────

    [Fact]
    public void Follow_WithFollowSelect_ProjectsColumns()
    {
        var r = _engine.Execute(
            "get users where name = 'Alice' follow users._id -> orders.user_id as orders select product, status",
            "testdb");

        Assert.NotNull(r.Data);
        Assert.Equal(2, r.Data.Count);
        var row = r.Data[0];

        // Selected columns should be present
        Assert.True(row.ContainsKey("orders.product"));
        Assert.True(row.ContainsKey("orders.status"));

        // Non-selected columns should be absent (including _id)
        Assert.False(row.ContainsKey("orders._id"));
        Assert.False(row.ContainsKey("orders.amount"));
        Assert.False(row.ContainsKey("orders.user_id"));
    }

    [Fact]
    public void Follow_WithFollowSelect_AndWhere()
    {
        var r = _engine.Execute(
            "get users where name = 'Alice' follow users._id -> orders.user_id as orders select product where status = 'completed'",
            "testdb");

        Assert.NotNull(r.Data);
        Assert.Single(r.Data);
        var row = r.Data[0];
        Assert.Equal("Widget", (string?)row["orders.product"]);
        Assert.False(row.ContainsKey("orders.amount"));
        Assert.False(row.ContainsKey("orders.status")); // not in follow select
    }

    [Fact]
    public void Follow_WithFollowSelect_ChainedFollows()
    {
        _engine.Execute("create table tags (order_id ulong, tag string 50, priority sint)", "testdb");
        _engine.Execute("upsert tags {order_id: 1, tag: 'urgent', priority: 1}", "testdb");

        var r = _engine.Execute(
            "get users where name = 'Alice' follow users._id -> orders.user_id as orders select product where status = 'completed' follow orders._id -> tags.order_id as tags select tag",
            "testdb");

        Assert.NotNull(r.Data);
        Assert.Single(r.Data);
        var row = r.Data[0];

        // First follow: only product
        Assert.True(row.ContainsKey("orders.product"));
        Assert.False(row.ContainsKey("orders.amount"));

        // Second follow: only tag (no _id since not in select)
        Assert.True(row.ContainsKey("tags.tag"));
        Assert.False(row.ContainsKey("tags._id"));
        Assert.False(row.ContainsKey("tags.priority"));
    }

    // ── Post-follow select ──────────────────────────────────

    [Fact]
    public void PostFollow_Select_FiltersFlatResult()
    {
        var r = _engine.Execute(
            "get users follow users._id -> orders.user_id as orders select name, orders.product",
            "testdb");

        Assert.NotNull(r.Data);
        Assert.Equal(4, r.Data.Count);

        var row = r.Data[0];
        Assert.True(row.ContainsKey("name"));
        Assert.True(row.ContainsKey("orders.product"));
        // Excluded columns should not be present
        Assert.False(row.ContainsKey("_id"));
        Assert.False(row.ContainsKey("email"));
        Assert.False(row.ContainsKey("orders._id"));
        Assert.False(row.ContainsKey("orders.amount"));
    }

    [Fact]
    public void PostFollow_Select_WithAlias()
    {
        var r = _engine.Execute(
            "get users follow users._id -> orders.user_id as orders select name, orders.product as item",
            "testdb");

        Assert.NotNull(r.Data);
        var row = r.Data[0];
        Assert.True(row.ContainsKey("name"));
        Assert.True(row.ContainsKey("item"));
        Assert.False(row.ContainsKey("orders.product"));
    }

    [Fact]
    public void PostFollow_ExcludeSelect_RemovesColumns()
    {
        var r = _engine.Execute(
            "get users follow users._id -> orders.user_id as orders -select email, orders.status",
            "testdb");

        Assert.NotNull(r.Data);
        Assert.Equal(4, r.Data.Count);

        var row = r.Data[0];
        Assert.True(row.ContainsKey("name"));
        Assert.True(row.ContainsKey("orders.product"));
        // Excluded columns
        Assert.False(row.ContainsKey("email"));
        Assert.False(row.ContainsKey("orders.status"));
    }

    [Fact]
    public void PostFollow_Select_DotNotation_Id()
    {
        var r = _engine.Execute(
            "get users follow users._id -> orders.user_id as orders select _id, orders._id",
            "testdb");

        Assert.NotNull(r.Data);
        var row = r.Data[0];
        Assert.Equal(2, row.Count);
        Assert.True(row.ContainsKey("_id"));
        Assert.True(row.ContainsKey("orders._id"));
    }
}

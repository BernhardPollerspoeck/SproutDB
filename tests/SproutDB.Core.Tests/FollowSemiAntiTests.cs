namespace SproutDB.Core.Tests;

/// <summary>
/// Semi (<c>-?&gt;</c>) and anti (<c>-!&gt;</c>) follows — alone, with follow-where,
/// with null/dangling keys and inside multi-follow chains.
/// </summary>
public class FollowSemiAntiTests : IDisposable
{
    private readonly string _tempDir;
    private readonly SproutEngine _engine;

    public FollowSemiAntiTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), $"sproutdb-test-{Guid.NewGuid()}");
        _engine = new SproutEngine(_tempDir);
        _engine.ExecuteOne("create database", "testdb");

        // users: 1 Alice, 2 Bob, 3 Carol, 4 Dave, 5 Eve
        Exec("create table users (name string 50)");
        foreach (var name in new[] { "Alice", "Bob", "Carol", "Dave", "Eve" })
            Exec($"upsert users {{name: '{name}'}}");

        // roles: 1 admin, 2 editor, 3 viewer
        Exec("create table roles (name string 50)");
        foreach (var name in new[] { "admin", "editor", "viewer" })
            Exec($"upsert roles {{name: '{name}'}}");

        // user_roles: Alice admin+editor, Bob editor twice (duplicate), Carol viewer, Eve without role
        Exec("create table user_roles (user_id ulong, role_id ulong)");
        Exec("upsert user_roles {user_id: 1, role_id: 1}");
        Exec("upsert user_roles {user_id: 1, role_id: 2}");
        Exec("upsert user_roles {user_id: 2, role_id: 2}");
        Exec("upsert user_roles {user_id: 2, role_id: 2}");
        Exec("upsert user_roles {user_id: 3, role_id: 3}");
        Exec("upsert user_roles {user_id: 5}");

        // orders: 1..4 regular, 5 without user, 6 dangling user, 7 Bob without items
        Exec("create table orders (user_id ulong, status string 20, amount double)");
        Exec("upsert orders {user_id: 1, status: 'completed', amount: 100}");
        Exec("upsert orders {user_id: 1, status: 'pending', amount: 30}");
        Exec("upsert orders {user_id: 2, status: 'completed', amount: 50}");
        Exec("upsert orders {user_id: 3, status: 'cancelled', amount: 20}");
        Exec("upsert orders {status: 'completed', amount: 999}");
        Exec("upsert orders {user_id: 99, status: 'completed', amount: 10}");
        Exec("upsert orders {user_id: 2, status: 'pending', amount: 5}");

        // order_items
        Exec("create table order_items (order_id ulong, product_id ulong, quantity uint)");
        Exec("upsert order_items {order_id: 1, product_id: 1, quantity: 2}");
        Exec("upsert order_items {order_id: 1, product_id: 2, quantity: 1}");
        Exec("upsert order_items {order_id: 2, product_id: 2, quantity: 5}");
        Exec("upsert order_items {order_id: 3, product_id: 3, quantity: 1}");
        Exec("upsert order_items {order_id: 4, product_id: 1, quantity: 1}");

        // products: 1 Widget tools, 2 Gadget toys, 3 Gizmo tools
        Exec("create table products (name string 50, category string 20)");
        Exec("upsert products {name: 'Widget', category: 'tools'}");
        Exec("upsert products {name: 'Gadget', category: 'toys'}");
        Exec("upsert products {name: 'Gizmo', category: 'tools'}");
    }

    public void Dispose()
    {
        _engine.Dispose();
        if (Directory.Exists(_tempDir))
            Directory.Delete(_tempDir, true);
    }

    private void Exec(string query)
    {
        var r = _engine.ExecuteOne(query, "testdb");
        Assert.True(r.Errors is null, $"{query} → {r.Errors?[0].Message}");
    }

    private List<Dictionary<string, object?>> Rows(string query)
    {
        var r = _engine.ExecuteOne(query, "testdb");
        Assert.True(r.Errors is null, $"{query} → {r.Errors?[0].Message}");
        Assert.NotNull(r.Data);
        return r.Data;
    }

    private static string Col(List<Dictionary<string, object?>> rows, string key)
        => string.Join(",", rows.Select(r => r.TryGetValue(key, out var v) ? v?.ToString() ?? "null" : "<missing>"));

    private SproutError Error(string query)
    {
        var r = _engine.ExecuteOne(query, "testdb");
        Assert.Equal(SproutOperation.Error, r.Operation);
        Assert.NotNull(r.Errors);
        return Assert.Single(r.Errors);
    }

    // ── Single semi / anti ──────────────────────────────────

    [Fact]
    public void Semi_ReturnsEachSourceRowOnce()
    {
        var rows = Rows("get users follow users._id -?> orders.user_id");

        // Alice has two orders but appears once
        Assert.Equal("Alice,Bob,Carol", Col(rows, "name"));
    }

    [Fact]
    public void Semi_AddsNoTargetColumns()
    {
        var rows = Rows("get users follow users._id -?> orders.user_id as o");

        Assert.All(rows, row => Assert.Equal(["_id", "name"], row.Keys));
    }

    [Fact]
    public void Anti_ReturnsRowsWithoutMatch()
    {
        var rows = Rows("get users follow users._id -!> orders.user_id");

        Assert.Equal("Dave,Eve", Col(rows, "name"));
        Assert.All(rows, row => Assert.Equal(["_id", "name"], row.Keys));
    }

    [Fact]
    public void Semi_FollowWhere_PreFiltersTarget()
    {
        var rows = Rows("get users follow users._id -?> orders.user_id where orders.status = 'completed'");

        Assert.Equal("Alice,Bob", Col(rows, "name"));
    }

    [Fact]
    public void Anti_FollowWhere_PreFiltersTarget()
    {
        var rows = Rows("get users follow users._id -!> orders.user_id where orders.status = 'completed'");

        Assert.Equal("Carol,Dave,Eve", Col(rows, "name"));
    }

    [Fact]
    public void Semi_WithAlias_WherePrefixIsAlias()
    {
        var rows = Rows("get users follow users._id -?> orders.user_id as o where o.status = 'pending' and o.amount > 10");

        Assert.Equal("Alice", Col(rows, "name"));
    }

    [Theory]
    [InlineData("")]
    [InlineData(" where orders.status = 'completed'")]
    [InlineData(" where orders.amount >= 50")]
    [InlineData(" where orders.status = 'nope'")]
    public void SemiAndAnti_PartitionTheSource(string followWhere)
    {
        var all = Rows("get users");
        var semi = Rows($"get users follow users._id -?> orders.user_id{followWhere}");
        var anti = Rows($"get users follow users._id -!> orders.user_id{followWhere}");

        var ids = semi.Concat(anti).Select(r => r["_id"]).Order().ToList();
        Assert.Equal(all.Select(r => r["_id"]).Order().ToList(), ids);
    }

    [Fact]
    public void Semi_NullAndDanglingSourceKeys_AreDropped()
    {
        var rows = Rows("get orders follow orders.user_id -?> users._id");

        Assert.Equal("1,2,3,4,7", Col(rows, "_id"));
    }

    [Fact]
    public void Anti_NullAndDanglingSourceKeys_AreKept()
    {
        var rows = Rows("get orders follow orders.user_id -!> users._id");

        Assert.Equal("5,6", Col(rows, "_id"));
    }

    [Fact]
    public void Semi_ManyToOneDirection_WithTargetWhere()
    {
        var rows = Rows("get orders follow orders.user_id -?> users._id where users.name = 'Alice'");

        Assert.Equal("1,2", Col(rows, "_id"));
    }

    [Fact]
    public void Semi_BaseWhereAndSelect_StillApply()
    {
        var rows = Rows("get users select name where name != 'Alice' follow users._id -?> orders.user_id");

        Assert.Equal("Bob,Carol", Col(rows, "name"));
        // the source column needed for the join is not leaked into the result
        Assert.All(rows, row => Assert.Equal(["name"], row.Keys));
    }

    [Fact]
    public void Semi_KeepsBaseOrder()
    {
        var rows = Rows("get users order by name desc follow users._id -?> orders.user_id");

        Assert.Equal("Carol,Bob,Alice", Col(rows, "name"));
    }

    [Fact]
    public void Semi_Count()
    {
        var r = _engine.ExecuteOne("get users follow users._id -?> orders.user_id count", "testdb");

        Assert.Null(r.Errors);
        Assert.Equal(3, r.Affected);
    }

    [Fact]
    public void Anti_Limit_AppliesAfterFilter()
    {
        var rows = Rows("get users follow users._id -!> orders.user_id limit 1");

        Assert.Equal("Dave", Col(rows, "name"));
    }

    [Fact]
    public void Semi_WithIndexedTargetColumn_SameResult()
    {
        Exec("create index orders.user_id");

        var rows = Rows("get users follow users._id -?> orders.user_id where orders.status = 'completed'");

        Assert.Equal("Alice,Bob", Col(rows, "name"));
    }

    // ── Multi-follow chains ─────────────────────────────────

    [Fact]
    public void Chain_InnerThenSemi_FiltersJoinedRows()
    {
        // users with the admin role via the junction table
        var rows = Rows(
            "get users " +
            "follow users._id -> user_roles.user_id as ur " +
            "follow ur.role_id -?> roles._id where roles.name = 'admin'");

        Assert.Equal("Alice", Col(rows, "name"));
        // columns of the inner follow stay, the semi adds none
        var row = Assert.Single(rows);
        Assert.True(row.ContainsKey("ur.role_id"));
        Assert.DoesNotContain(row.Keys, k => k.StartsWith("roles."));
    }

    [Fact]
    public void Chain_InnerThenSemi_WorksPerJoinedRow_NotPerBaseRow()
    {
        // Semi after an inner follow keeps every (user, user_role) pair with a match:
        // Alice has editor once, Bob has it twice
        var rows = Rows(
            "get users " +
            "follow users._id -> user_roles.user_id as ur " +
            "follow ur.role_id -?> roles._id where roles.name = 'editor'");

        Assert.Equal("Alice,Bob,Bob", Col(rows, "name"));
    }

    [Fact]
    public void Chain_InnerThenSemi_DedupByBaseId_OneRowPerUser()
    {
        var rows = Rows(
            "get users " +
            "follow users._id -> user_roles.user_id as ur " +
            "follow ur.role_id -?> roles._id where roles.name = 'editor' " +
            "dedup by _id");

        Assert.Equal("Alice,Bob", Col(rows, "name"));
    }

    [Fact]
    public void Chain_InnerThenAnti_OrdersWithoutItems()
    {
        var rows = Rows(
            "get users " +
            "follow users._id -> orders.user_id as o " +
            "follow o._id -!> order_items.order_id");

        // only Bob's order 7 has no items (orders 5/6 have no user)
        var row = Assert.Single(rows);
        Assert.Equal("Bob", row["name"]);
        Assert.Equal(7UL, row["o._id"]);
    }

    [Fact]
    public void Chain_LeftThenSemi_DropsNullJoinRows()
    {
        var rows = Rows(
            "get users " +
            "follow users._id ->? orders.user_id as o " +
            "follow o._id -?> order_items.order_id where order_items.quantity >= 2");

        Assert.Equal("Alice,Alice", Col(rows, "name"));
        Assert.Equal("1,2", Col(rows, "o._id"));
    }

    [Fact]
    public void Chain_LeftThenAnti_KeepsNullJoinRows()
    {
        var rows = Rows(
            "get users " +
            "follow users._id ->? orders.user_id as o " +
            "follow o._id -!> order_items.order_id where order_items.quantity >= 2");

        Assert.Equal("Bob:3,Bob:7,Carol:4,Dave:null,Eve:null",
            string.Join(",", rows
                .Select(r => $"{r["name"]}:{r["o._id"]?.ToString() ?? "null"}")
                .Order(StringComparer.Ordinal)));
    }

    [Fact]
    public void Chain_SemiThenInnerFromBase()
    {
        var rows = Rows(
            "get users " +
            "follow users._id -?> orders.user_id where orders.status = 'completed' " +
            "follow users._id -> user_roles.user_id as ur");

        // Alice: 2 roles, Bob: duplicate editor assignment
        Assert.Equal("Alice,Alice,Bob,Bob", Col(rows, "name"));
        Assert.All(rows, row => Assert.DoesNotContain(row.Keys, k => k.StartsWith("orders.")));
    }

    [Fact]
    public void Chain_TwoSemis_AndSemantics()
    {
        var rows = Rows(
            "get users " +
            "follow users._id -?> orders.user_id " +
            "follow users._id -?> user_roles.user_id");

        Assert.Equal("Alice,Bob,Carol", Col(rows, "name"));
    }

    [Fact]
    public void Chain_SemiAndAnti_HasRoleRowButNoOrders()
    {
        var rows = Rows(
            "get users " +
            "follow users._id -?> user_roles.user_id " +
            "follow users._id -!> orders.user_id");

        Assert.Equal("Eve", Col(rows, "name"));
    }

    [Fact]
    public void Chain_ThreeLevels_SemiOnLastHop()
    {
        // users who ordered a product of category 'toys'
        var rows = Rows(
            "get users " +
            "follow users._id -> orders.user_id as o " +
            "follow o._id -> order_items.order_id as oi " +
            "follow oi.product_id -?> products._id where products.category = 'toys' " +
            "select name, o._id, oi.quantity");

        Assert.Equal("Alice:1:1,Alice:2:5",
            string.Join(",", rows.Select(r => $"{r["name"]}:{r["o._id"]}:{r["oi.quantity"]}")));
        Assert.All(rows, row => Assert.Equal(["name", "o._id", "oi.quantity"], row.Keys));
    }

    [Fact]
    public void Chain_ThreeLevels_AntiOnLastHop_WithDedup()
    {
        // users with at least one order item that is not a 'tools' product
        var rows = Rows(
            "get users " +
            "follow users._id -> orders.user_id as o " +
            "follow o._id -> order_items.order_id as oi " +
            "follow oi.product_id -!> products._id where products.category = 'tools' " +
            "dedup by _id");

        Assert.Equal("Alice", Col(rows, "name"));
    }

    [Fact]
    public void Chain_SemiBeforeRightJoin_RightJoinSeesFilteredRows()
    {
        // semi keeps Alice/Bob/Carol, then the right join adds unmatched roles
        var rows = Rows(
            "get users " +
            "follow users._id -?> orders.user_id " +
            "follow users._id ?-> user_roles.user_id as ur");

        Assert.Equal(6, rows.Count);
        // Eve's user_roles row has no surviving user → null base columns
        Assert.Contains(rows, r => r["name"] is null && Equals(r["ur.user_id"], 5UL));
    }

    [Fact]
    public void Chain_PostFollowOrderBy_OnSemiChain()
    {
        var rows = Rows(
            "get users " +
            "follow users._id -> orders.user_id as o " +
            "follow o._id -?> order_items.order_id " +
            "select name, o.amount order by o.amount desc");

        Assert.Equal("100,50,30,20", Col(rows, "o.amount"));
    }

    // ── Errors ──────────────────────────────────────────────

    [Theory]
    [InlineData("-?>")]
    [InlineData("-!>")]
    public void Error_SelectOnFilterFollow(string arrow)
    {
        var err = Error($"get users follow users._id {arrow} orders.user_id as o select status");

        Assert.Equal("SYNTAX_ERROR", err.Code);
        Assert.Contains($"'select' is not allowed on a '{arrow}' follow", err.Message);
    }

    [Fact]
    public void Error_FollowFromSemiAlias()
    {
        var err = Error(
            "get users follow users._id -?> orders.user_id as o " +
            "follow o._id -> order_items.order_id as oi");

        Assert.Equal("SYNTAX_ERROR", err.Code);
        Assert.Contains("cannot follow from 'o'", err.Message);
    }

    [Fact]
    public void Error_FollowFromAntiTableName()
    {
        var err = Error(
            "get users follow users._id -!> orders.user_id " +
            "follow orders._id -> order_items.order_id as oi");

        Assert.Equal("SYNTAX_ERROR", err.Code);
        Assert.Contains("cannot follow from 'orders'", err.Message);
    }

    [Fact]
    public void FollowFromReusedAlias_LatestFollowDecides()
    {
        // 'o' is first a semi alias, then re-bound by an inner follow — following from it is fine
        var rows = Rows(
            "get users " +
            "follow users._id -?> user_roles.user_id as o " +
            "follow users._id -> orders.user_id as o " +
            "follow o._id -?> order_items.order_id");

        Assert.Equal("Alice,Alice,Bob,Carol", Col(rows, "name"));
    }

    [Fact]
    public void Error_UnknownColumnInSemiWhere()
    {
        var err = Error("get users follow users._id -?> orders.user_id where orders.nope = 1");

        Assert.Equal("UNKNOWN_COLUMN", err.Code);
    }

    [Fact]
    public void Error_UnknownTargetTable()
    {
        var err = Error("get users follow users._id -!> nope.user_id");

        Assert.Equal("UNKNOWN_TABLE", err.Code);
    }

    [Fact]
    public void Error_InnerFollowStillRequiresAs()
    {
        var err = Error("get users follow users._id -> orders.user_id");

        Assert.Equal("SYNTAX_ERROR", err.Code);
        Assert.Contains("expected 'as'", err.Message);
    }
}

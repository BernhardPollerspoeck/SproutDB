namespace SproutDB.Core.Tests;

/// <summary>
/// ORDER BY on joined results: sorts after the follows, so followed columns
/// (alias.col) really sort. Before the fix the sort ran on the base rows and
/// silently ignored followed columns.
/// </summary>
public class FollowOrderByTests : IDisposable
{
    private readonly string _tempDir;
    private readonly SproutEngine _engine;

    public FollowOrderByTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), $"sproutdb-test-{Guid.NewGuid()}");
        _engine = new SproutEngine(_tempDir);
        _engine.ExecuteOne("create database", "testdb");

        Exec("create table users (name string 50)");
        Exec("upsert users {name: 'Alice'}");
        Exec("upsert users {name: 'Bob'}");
        Exec("upsert users {name: 'Carol'}");

        Exec("create table orders (user_id ulong, amount double)");
        Exec("upsert orders {user_id: 1, amount: 100}"); // 1
        Exec("upsert orders {user_id: 1, amount: 30}");  // 2
        Exec("upsert orders {user_id: 2, amount: 50}");  // 3
        Exec("upsert orders {user_id: 1, amount: 70}");  // 4
        Exec("upsert orders {user_id: 2, amount: 80}");  // 5

        Exec("create table items (order_id ulong, sku string 20)");
        Exec("upsert items {order_id: 5, sku: 'b'}");
        Exec("upsert items {order_id: 1, sku: 'c'}");
        Exec("upsert items {order_id: 1, sku: 'a'}");
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

    private SproutResponse Ok(string query)
    {
        var r = _engine.ExecuteOne(query, "testdb");
        Assert.True(r.Errors is null, $"{query} → {r.Errors?[0].Message}");
        Assert.NotNull(r.Data);
        return r;
    }

    private List<Dictionary<string, object?>> Rows(string query)
    {
        var r = Ok(query);
        Assert.NotNull(r.Data);
        return r.Data;
    }

    private static string Col(List<Dictionary<string, object?>> rows, string key)
        => string.Join(",", rows.Select(r => r.TryGetValue(key, out var v) ? v?.ToString() ?? "null" : "<missing>"));

    [Fact]
    public void OrderBy_FollowedColumn_Desc()
    {
        var rows = Rows("get users follow users._id -> orders.user_id as o order by o.amount desc");

        Assert.Equal("100,80,70,50,30", Col(rows, "o.amount"));
    }

    [Fact]
    public void OrderBy_FollowedColumn_NotInPostFollowSelect()
    {
        var rows = Rows("get users follow users._id -> orders.user_id as o select name, o._id order by o.amount desc");

        Assert.Equal("Alice,Bob,Alice,Bob,Alice", Col(rows, "name"));
        Assert.Equal("1,5,4,3,2", Col(rows, "o._id"));
        Assert.All(rows, row => Assert.Equal(new[] { "name", "o._id" }, row.Keys));
    }

    [Fact]
    public void OrderBy_PostFollowSelectAlias()
    {
        var rows = Rows("get users follow users._id -> orders.user_id as o select name, o.amount as amt order by amt");

        Assert.Equal("30,50,70,80,100", Col(rows, "amt"));
    }

    [Fact]
    public void OrderBy_BaseColumn_IsStable_KeepsJoinOrderWithinParent()
    {
        var rows = Rows("get users follow users._id -> orders.user_id as o order by name desc");

        Assert.Equal("3,5,1,2,4", Col(rows, "o._id"));
    }

    [Fact]
    public void OrderBy_BaseThenFollowed()
    {
        var rows = Rows("get users follow users._id -> orders.user_id as o order by name, o.amount desc");

        Assert.Equal("Alice:100,Alice:70,Alice:30,Bob:80,Bob:50",
            string.Join(",", rows.Select(r => $"{r["name"]}:{r["o.amount"]}")));
    }

    [Fact]
    public void OrderBy_LeftJoin_NullsFirstAscending()
    {
        var rows = Rows("get users follow users._id ->? orders.user_id as o order by o.amount");

        Assert.Equal("Carol", rows[0]["name"]);
        Assert.Equal("null,30,50,70,80,100", Col(rows, "o.amount"));
    }

    [Fact]
    public void OrderBy_SecondFollowColumn_InChain()
    {
        var rows = Rows(
            "get users " +
            "follow users._id -> orders.user_id as o " +
            "follow o._id -> items.order_id as i " +
            "order by i.sku");

        Assert.Equal("a,b,c", Col(rows, "i.sku"));
        Assert.Equal("1,5,1", Col(rows, "o._id"));
    }

    [Fact]
    public void OrderBy_FollowedColumn_WithLimit()
    {
        var rows = Rows("get users follow users._id -> orders.user_id as o order by o.amount desc limit 2");

        Assert.Equal("100,80", Col(rows, "o.amount"));
    }

    [Fact]
    public void OrderBy_FollowedColumn_WithPaging()
    {
        var page2 = Ok("get users follow users._id -> orders.user_id as o order by o.amount desc page 2 size 2");

        Assert.NotNull(page2.Data);
        Assert.Equal("70,50", Col(page2.Data, "o.amount"));
    }
}

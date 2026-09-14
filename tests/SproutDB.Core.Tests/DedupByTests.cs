namespace SproutDB.Core.Tests;

/// <summary>
/// <c>dedup by</c>: first result row per key, in result order (after ORDER BY),
/// before count/limit/paging — with and without follow.
/// </summary>
public class DedupByTests : IDisposable
{
    private readonly string _tempDir;
    private readonly SproutEngine _engine;

    public DedupByTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), $"sproutdb-test-{Guid.NewGuid()}");
        _engine = new SproutEngine(_tempDir);
        _engine.ExecuteOne("create database", "testdb");

        Exec("create table users (name string 50)");
        Exec("upsert users {name: 'Alice'}");
        Exec("upsert users {name: 'Bob'}");
        Exec("upsert users {name: 'Carol'}");

        Exec("create table orders (user_id ulong, status string 20, amount double)");
        Exec("upsert orders {user_id: 1, status: 'completed', amount: 100}"); // 1
        Exec("upsert orders {user_id: 1, status: 'pending', amount: 30}");    // 2
        Exec("upsert orders {user_id: 2, status: 'completed', amount: 50}");  // 3
        Exec("upsert orders {user_id: 1, status: 'completed', amount: 70}");  // 4
        Exec("upsert orders {user_id: 2, status: 'completed', amount: 80}");  // 5
        Exec("upsert orders {status: 'completed', amount: 5}");               // 6
        Exec("upsert orders {status: 'pending', amount: 6}");                 // 7
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

    private SproutError Error(string query)
    {
        var r = _engine.ExecuteOne(query, "testdb");
        Assert.Equal(SproutOperation.Error, r.Operation);
        Assert.NotNull(r.Errors);
        return Assert.Single(r.Errors);
    }

    // ── Without follow ──────────────────────────────────────

    [Fact]
    public void Dedup_FirstRowPerKey_AfterOrderBy()
    {
        var rows = Rows("get orders order by amount desc dedup by user_id");

        // highest order per user; the two orders without user share the null key
        Assert.Equal("100,80,6", Col(rows, "amount"));
    }

    [Fact]
    public void Dedup_WithoutOrderBy_UsesSlotOrder()
    {
        var rows = Rows("get orders dedup by user_id");

        Assert.Equal("1,3,6", Col(rows, "_id"));
    }

    [Fact]
    public void Dedup_ClausePositionInQueryText_DoesNotMatter()
    {
        var rows = Rows("get orders dedup by user_id order by amount desc");

        Assert.Equal("100,80,6", Col(rows, "amount"));
    }

    [Fact]
    public void Dedup_MultipleColumns()
    {
        var rows = Rows("get orders order by _id dedup by user_id, status");

        Assert.Equal("1,2,3,6,7", Col(rows, "_id"));
    }

    [Fact]
    public void Dedup_KeepsAllColumns()
    {
        var row = Assert.Single(Rows("get orders where user_id = 2 order by amount desc dedup by user_id"));

        Assert.Equal(5UL, row["_id"]);
        Assert.Equal("completed", row["status"]);
        Assert.Equal(80.0, row["amount"]);
    }

    [Fact]
    public void Dedup_Count_CountsDedupedRows()
    {
        var r = _engine.ExecuteOne("get orders dedup by user_id count", "testdb");

        Assert.Null(r.Errors);
        Assert.Equal(3, r.Affected);
    }

    [Fact]
    public void Dedup_Limit_AppliesAfterDedup()
    {
        var rows = Rows("get orders order by amount desc dedup by user_id limit 3");

        Assert.Equal("100,80,6", Col(rows, "amount"));
    }

    [Fact]
    public void Dedup_OrderByIdLimit_DoesNotUseTopNShortcut()
    {
        // a top-2 shortcut would pick orders 7 and 6 (same null key) and return a single row
        var rows = Rows("get orders order by _id desc dedup by user_id limit 2");

        Assert.Equal("7,5", Col(rows, "_id"));
    }

    [Fact]
    public void Dedup_Paging_TotalIsDedupedCount()
    {
        var r = Ok("get orders order by amount desc dedup by user_id page 1 size 2");

        Assert.NotNull(r.Data);
        Assert.Equal("100,80", Col(r.Data, "amount"));
        Assert.NotNull(r.Paging);
        Assert.Equal(3, r.Paging.Total);
    }

    [Fact]
    public void Dedup_WithDistinct()
    {
        var rows = Rows("get orders select user_id, status distinct dedup by user_id");

        Assert.Equal(3, rows.Count);
    }

    [Fact]
    public void Dedup_BySelectAlias()
    {
        var rows = Rows("get orders select user_id as uid, amount order by amount desc dedup by uid");

        Assert.Equal("1,2,null", Col(rows, "uid"));
    }

    [Fact]
    public void Dedup_WithWhere()
    {
        var rows = Rows("get orders where status = 'completed' order by amount dedup by user_id");

        Assert.Equal("5,50,70", Col(rows, "amount"));
    }

    // ── With follow ─────────────────────────────────────────

    [Fact]
    public void Follow_TopRowPerBaseRow()
    {
        var rows = Rows("get users follow users._id -> orders.user_id as o order by o.amount desc dedup by _id");

        Assert.Equal("Alice:100,Bob:80", string.Join(",", rows.Select(r => $"{r["name"]}:{r["o.amount"]}")));
    }

    [Fact]
    public void Follow_DedupByFollowedColumn()
    {
        var rows = Rows("get users follow users._id -> orders.user_id as o order by o._id dedup by o.status");

        Assert.Equal("1,2", Col(rows, "o._id"));
    }

    [Fact]
    public void Follow_DedupByPostFollowSelectAlias()
    {
        var rows = Rows("get users follow users._id -> orders.user_id as o select name, o.amount as amt order by amt desc dedup by name");

        Assert.Equal("Alice:100,Bob:80", string.Join(",", rows.Select(r => $"{r["name"]}:{r["amt"]}")));
    }

    [Fact]
    public void Follow_DedupByAliasedFollowSelectColumn()
    {
        var rows = Rows("get users follow users._id -> orders.user_id as o select status as st order by _id dedup by st");

        Assert.Equal("completed,pending", Col(rows, "st"));
    }

    [Fact]
    public void Follow_LeftJoin_DedupKeepsUnmatchedRow()
    {
        var rows = Rows("get users follow users._id ->? orders.user_id as o dedup by _id");

        Assert.Equal("Alice,Bob,Carol", Col(rows, "name"));
    }

    [Fact]
    public void Follow_DedupThenCount()
    {
        var r = _engine.ExecuteOne("get users follow users._id -> orders.user_id as o dedup by _id count", "testdb");

        Assert.Null(r.Errors);
        Assert.Equal(2, r.Affected);
    }

    [Fact]
    public void Follow_ChainedFollows_DedupByMiddleAlias()
    {
        Exec("create table payments (order_id ulong, method string 20)");
        Exec("upsert payments {order_id: 1, method: 'card'}");
        Exec("upsert payments {order_id: 1, method: 'voucher'}");
        Exec("upsert payments {order_id: 3, method: 'card'}");

        // one row per order, even though order 1 has two payments
        var rows = Rows(
            "get users " +
            "follow users._id -> orders.user_id as o " +
            "follow o._id -> payments.order_id as p " +
            "order by o._id dedup by o._id");

        Assert.Equal("1,3", Col(rows, "o._id"));
        Assert.Equal("card,card", Col(rows, "p.method"));
    }

    // ── Errors ──────────────────────────────────────────────

    [Fact]
    public void Error_ColumnNotInSelect()
    {
        var err = Error("get orders select amount dedup by user_id");

        Assert.Equal("UNKNOWN_COLUMN", err.Code);
        Assert.Contains("'dedup by user_id' requires 'user_id' in the select list", err.Message);
    }

    [Fact]
    public void Error_ColumnExcludedBySelect()
    {
        var err = Error("get orders -select user_id dedup by user_id");

        Assert.Contains("requires 'user_id' in the select list", err.Message);
    }

    [Fact]
    public void Error_UnknownColumn()
    {
        var err = Error("get orders dedup by nope");

        Assert.Equal("UNKNOWN_COLUMN", err.Code);
        Assert.Contains("column 'nope' does not exist", err.Message);
    }

    [Fact]
    public void Error_Follow_ColumnNotInPostFollowSelect()
    {
        var err = Error("get users follow users._id -> orders.user_id as o select name, o.amount dedup by o.status");

        Assert.Contains("requires 'o.status' in the select list", err.Message);
    }

    [Fact]
    public void Error_Follow_ColumnNotInFollowSelect()
    {
        var err = Error("get users follow users._id -> orders.user_id as o select amount dedup by o.status");

        Assert.Equal("UNKNOWN_COLUMN", err.Code);
        Assert.Contains("column 'o.status' is not in the result", err.Message);
    }

    [Fact]
    public void Error_Follow_SemiAliasHasNoColumns()
    {
        var err = Error("get users follow users._id -?> orders.user_id as o dedup by o.status");

        Assert.Contains("'o' is not the alias of a column-producing follow", err.Message);
    }

    [Theory]
    [InlineData("get orders group by status dedup by status", "'dedup by' cannot be combined with 'group by'")]
    [InlineData("get orders sum amount dedup by user_id", "'dedup by' cannot be combined with aggregate functions")]
    [InlineData("get orders after '0' dedup by user_id", "'after' cannot be combined with 'dedup by'")]
    [InlineData("get orders dedup by user_id dedup by status", "'dedup by' may appear only once")]
    [InlineData("get orders dedup user_id", "expected 'by' after 'dedup'")]
    [InlineData("get orders dedup by", "expected column name")]
    public void Error_Syntax(string query, string message)
    {
        var err = Error(query);

        Assert.Equal("SYNTAX_ERROR", err.Code);
        Assert.Contains(message, err.Message, StringComparison.OrdinalIgnoreCase);
    }
}

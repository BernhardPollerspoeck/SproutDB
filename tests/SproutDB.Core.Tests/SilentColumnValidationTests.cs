namespace SproutDB.Core.Tests;

/// <summary>
/// Column references that used to be ignored without an error — the result silently
/// lacked the sort or the column. Each must now fail with UNKNOWN_COLUMN:
/// order by on grouped results, order by on joined rows, post-follow select.
/// </summary>
public class SilentColumnValidationTests : IDisposable
{
    private readonly string _tempDir;
    private readonly SproutEngine _engine;

    public SilentColumnValidationTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), $"sproutdb-test-{Guid.NewGuid()}");
        _engine = new SproutEngine(_tempDir);
        _engine.ExecuteOne("create database", "testdb");

        Exec("create table artikel (sku string 20, name string 50, gruppe string 10, preis double)");
        Exec("upsert artikel {sku: 'S1', name: 'Zange', gruppe: 'G1', preis: 10}");
        Exec("upsert artikel {sku: 'S2', name: 'Bohrer', gruppe: 'G2', preis: 30}");
        Exec("upsert artikel {sku: 'S3', name: 'Flansch', gruppe: 'G1', preis: 5}");

        Exec("create table users (name string 50, email string 100)");
        Exec("upsert users {name: 'Alice', email: 'a@x'}");
        Exec("upsert users {name: 'Bob', email: 'b@x'}");

        Exec("create table orders (user_id ulong, status string 20, amount double)");
        Exec("upsert orders {user_id: 1, status: 'completed', amount: 100}");
        Exec("upsert orders {user_id: 2, status: 'pending', amount: 50}");
        Exec("upsert orders {user_id: 1, status: 'pending', amount: 70}");
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

    private SproutError UnknownColumn(string query)
    {
        var r = _engine.ExecuteOne(query, "testdb");
        Assert.Equal(SproutOperation.Error, r.Operation);
        Assert.NotNull(r.Errors);
        var err = Assert.Single(r.Errors);
        Assert.Equal("UNKNOWN_COLUMN", err.Code);
        return err;
    }

    // ── order by on grouped results ─────────────────────────

    [Fact]
    public void GroupBy_OrderByUnknownColumn_IsError()
    {
        var err = UnknownColumn("get artikel group by gruppe order by nope");

        Assert.Contains("'order by nope' requires 'nope' in the result — group by returns only the group columns and 'count'", err.Message);
    }

    [Fact]
    public void GroupBy_OrderByUngroupedColumn_IsError()
    {
        // the ticket query: 'name' is gone after grouping, the sort silently did nothing
        const string query = "get artikel select sku, name group by sku order by name";
        var err = UnknownColumn(query);

        Assert.Contains("'order by name' requires 'name' in the result", err.Message);
        Assert.Equal(query.LastIndexOf("name", StringComparison.Ordinal), err.Position);
    }

    [Fact]
    public void GroupBy_WithAggregate_OrderByCount_IsError()
    {
        var err = UnknownColumn("get artikel sum preis as summe group by gruppe order by count");

        Assert.Contains("group by returns only the group columns and 'summe'", err.Message);
    }

    [Theory]
    [InlineData("get artikel group by gruppe order by gruppe desc", "gruppe", "G2,G1")]
    [InlineData("get artikel group by gruppe order by count desc", "gruppe", "G1,G2")]
    [InlineData("get artikel count group by gruppe order by count", "gruppe", "G2,G1")]
    [InlineData("get artikel sum preis as summe group by gruppe order by summe desc", "summe", "30,15")]
    [InlineData("get artikel sum preis group by gruppe order by sum", "sum", "15,30")]
    public void GroupBy_OrderByResultKey_Sorts(string query, string key, string expected)
    {
        Assert.Equal(expected, Col(Rows(query), key));
    }

    // ── order by on joined rows ─────────────────────────────

    [Fact]
    public void Follow_OrderByTypoInFollowedColumn_IsError()
    {
        var err = UnknownColumn("get users follow users._id -> orders.user_id as o order by o.amout desc");

        Assert.Contains("column 'amout' does not exist on 'orders'", err.Message);
    }

    [Fact]
    public void Follow_OrderByUnknownAlias_IsError()
    {
        var err = UnknownColumn("get users follow users._id -> orders.user_id as o order by x.amount");

        Assert.Contains("'x' is not the alias of a column-producing follow", err.Message);
    }

    [Fact]
    public void Follow_OrderByColumnOutsideFollowSelect_IsError()
    {
        var err = UnknownColumn("get users follow users._id -> orders.user_id as o select amount order by o.status");

        Assert.Contains("add 'status' to the select of follow 'o'", err.Message);
    }

    [Fact]
    public void Follow_OrderByBaseColumnNotSelectedBeforeFollow_IsError()
    {
        var err = UnknownColumn("get users select name follow users._id -> orders.user_id as o order by email");

        Assert.Contains("column 'email' is not in the joined rows — add it to the select before 'follow'", err.Message);
    }

    [Fact]
    public void Follow_OrderByJoinKeyOnlyNeededForJoin_IsError()
    {
        // _id is read internally for the join but removed afterwards — sorting by it did nothing
        var err = UnknownColumn("get users select name follow users._id -> orders.user_id as o order by _id");

        Assert.Contains("column '_id' is not in the joined rows", err.Message);
    }

    [Fact]
    public void Follow_OrderBySemiAliasColumn_IsError()
    {
        var err = UnknownColumn("get users follow users._id -?> orders.user_id as o order by o.amount");

        Assert.Contains("'o' is not the alias of a column-producing follow", err.Message);
    }

    [Fact]
    public void Follow_OrderByUnknownBareColumn_IsError()
    {
        var err = UnknownColumn("get users follow users._id -> orders.user_id as o order by nope");

        Assert.Contains("column 'nope' does not exist", err.Message);
    }

    [Fact]
    public void Follow_OrderByTypo_WithCount_IsError()
    {
        UnknownColumn("get users follow users._id -> orders.user_id as o order by o.amout count");
    }

    [Theory]
    [InlineData("get users follow users._id -> orders.user_id as o order by o.amount desc", "o.amount", "100,70,50")]
    [InlineData("get users follow users._id -> orders.user_id as o select name, o._id order by o.amount desc", "o._id", "1,3,2")]
    [InlineData("get users follow users._id -> orders.user_id as o select name, o.amount as amt order by amt", "amt", "50,70,100")]
    [InlineData("get users select name follow users._id -> orders.user_id as o order by name desc", "name", "Bob,Alice,Alice")]
    [InlineData("get users follow users._id -> orders.user_id as o select status as st order by st", "st", "completed,pending,pending")]
    public void Follow_OrderByJoinedKey_Sorts(string query, string key, string expected)
    {
        Assert.Equal(expected, Col(Rows(query), key));
    }

    // ── post-follow select ──────────────────────────────────

    [Fact]
    public void PostFollowSelect_TypoInFollowedColumn_IsError()
    {
        const string query = "get users follow users._id -> orders.user_id as o select name, o.amout";
        var err = UnknownColumn(query);

        Assert.Contains("column 'amout' does not exist on 'orders'", err.Message);
    }

    [Fact]
    public void PostFollowSelect_UnknownAlias_IsError()
    {
        var err = UnknownColumn("get users follow users._id -> orders.user_id as o select name, x.amount");

        Assert.Contains("'x' is not the alias of a column-producing follow", err.Message);
    }

    [Fact]
    public void PostFollowSelect_BaseTablePrefix_IsError()
    {
        // base columns are plain keys — 'users.name' never existed on the joined rows
        var err = UnknownColumn("get users follow users._id -> orders.user_id as o select users.name, o.amount");

        Assert.Contains("'users' is not the alias of a column-producing follow", err.Message);
    }

    [Fact]
    public void PostFollowSelect_BaseColumnNotSelectedBeforeFollow_IsError()
    {
        var err = UnknownColumn("get users select name follow users._id -> orders.user_id as o select email, o.amount");

        Assert.Contains("column 'email' is not in the joined rows", err.Message);
    }

    [Fact]
    public void PostFollowSelect_SemiAliasColumn_IsError()
    {
        UnknownColumn(
            "get users follow users._id -> orders.user_id as o " +
            "follow o.user_id -?> users._id as u select name, u.email");
    }

    [Fact]
    public void PostFollowExclude_UnknownColumn_IsError()
    {
        UnknownColumn("get users follow users._id -> orders.user_id as o -select o.nope");
    }

    [Fact]
    public void PostFollowSelect_ReportsEveryUnknownColumn()
    {
        var r = _engine.ExecuteOne("get users follow users._id -> orders.user_id as o select o.amout, x.y, name", "testdb");

        Assert.Equal(SproutOperation.Error, r.Operation);
        Assert.NotNull(r.Errors);
        Assert.Equal(2, r.Errors.Count);
        Assert.All(r.Errors, e => Assert.Equal("UNKNOWN_COLUMN", e.Code));
    }

    [Fact]
    public void PostFollowSelect_ValidKeys_Projects()
    {
        var rows = Rows("get users follow users._id -> orders.user_id as o select name, o.amount as amt, o._id, 1 as v");

        Assert.All(rows, row => Assert.Equal(new[] { "name", "amt", "o._id", "v" }, row.Keys));
    }

    [Fact]
    public void PostFollowExclude_ValidKey_Removes()
    {
        var rows = Rows("get users follow users._id -> orders.user_id as o -select o.status, email");

        Assert.All(rows, row =>
        {
            Assert.DoesNotContain("o.status", row.Keys);
            Assert.DoesNotContain("email", row.Keys);
            Assert.Contains("o.amount", row.Keys);
        });
    }
}

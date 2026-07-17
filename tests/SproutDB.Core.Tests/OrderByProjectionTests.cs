namespace SproutDB.Core.Tests;

/// <summary>
/// 'order by' runs on the projected rows. A sort column that exists in the table but not
/// in the result used to silently not sort at all — now it is rejected up front.
/// </summary>
public class OrderByProjectionTests : IDisposable
{
    private readonly string _tempDir;
    private readonly SproutEngine _engine;

    public OrderByProjectionTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), $"sproutdb-test-{Guid.NewGuid()}");
        _engine = new SproutEngine(_tempDir);
        _engine.ExecuteOne("create database", "testdb");

        _engine.ExecuteOne("create table routes (host string 50, port uint, active bool)", "testdb");
        _engine.ExecuteOne("upsert routes {host: 'a', port: 80, active: true}", "testdb");
        _engine.ExecuteOne("upsert routes {host: 'b', port: 443, active: false}", "testdb");
        _engine.ExecuteOne("upsert routes {host: 'c', port: 8080, active: true}", "testdb");
    }

    public void Dispose()
    {
        _engine.Dispose();
        if (Directory.Exists(_tempDir))
            Directory.Delete(_tempDir, true);
    }

    private static string Hosts(SproutResponse r)
    {
        Assert.NotNull(r.Data);
        return string.Join(",", r.Data.Select(row => row.TryGetValue("host", out var h) ? h : "?"));
    }

    // ── Rejected: sort column not in the result ───────────────

    [Fact]
    public void OrderBy_ColumnNotInSelect_Error()
    {
        var r = _engine.ExecuteOne("get routes select host order by port desc", "testdb");

        Assert.Equal(SproutOperation.Error, r.Operation);
        var err = Assert.Single(r.Errors!);
        Assert.Equal("UNKNOWN_COLUMN", err.Code);
        Assert.Contains("'order by port' requires 'port' in the select list", err.Message);
    }

    [Fact]
    public void OrderBy_ColumnRemovedByExcludeSelect_Error()
    {
        var r = _engine.ExecuteOne("get routes -select port order by port desc", "testdb");

        Assert.Equal(SproutOperation.Error, r.Operation);
        Assert.Contains("requires 'port' in the select list", r.Errors![0].Message);
    }

    [Fact]
    public void OrderBy_IdNotInSelect_WithoutLimit_Error()
    {
        // Without a limit there is no top-N fast path; the plain sort would see nothing.
        var r = _engine.ExecuteOne("get routes select host order by _id desc", "testdb");

        Assert.Equal(SproutOperation.Error, r.Operation);
        Assert.Contains("requires '_id' in the select list", r.Errors![0].Message);
    }

    [Fact]
    public void OrderBy_WithDistinct_ColumnNotInSelect_Error()
    {
        // After dedup on 'active' there is no single 'port' per row — ambiguous by design.
        var r = _engine.ExecuteOne("get routes select active distinct order by port", "testdb");

        Assert.Equal(SproutOperation.Error, r.Operation);
        Assert.Contains("requires 'port' in the select list", r.Errors![0].Message);
    }

    [Fact]
    public void OrderBy_IdWithDistinctAndLimit_Error()
    {
        // distinct disables the top-N fast path, so _id must be selected here too.
        var r = _engine.ExecuteOne("get routes select host distinct order by _id limit 2", "testdb");

        Assert.Equal(SproutOperation.Error, r.Operation);
        Assert.Contains("requires '_id' in the select list", r.Errors![0].Message);
    }

    [Fact]
    public void OrderBy_SecondColumnMissing_ErrorNamesIt()
    {
        var r = _engine.ExecuteOne("get routes select host order by host, port desc", "testdb");

        Assert.Equal(SproutOperation.Error, r.Operation);
        var err = Assert.Single(r.Errors!);
        Assert.Contains("'order by port'", err.Message);
    }

    [Fact]
    public void OrderBy_UnknownColumn_StillUnknownColumnError()
    {
        // A column that doesn't exist at all keeps its original message.
        var r = _engine.ExecuteOne("get routes select host order by missing", "testdb");

        Assert.Equal(SproutOperation.Error, r.Operation);
        Assert.Contains("column 'missing' does not exist", r.Errors![0].Message);
    }

    [Fact]
    public void OrderBy_OriginalNameOfAliasedColumn_Error()
    {
        // 'port as p' renames the row key to 'p' — there is no 'port' key to sort on.
        var r = _engine.ExecuteOne("get routes select host, port as p order by port", "testdb");

        Assert.Equal(SproutOperation.Error, r.Operation);
        Assert.Contains("requires 'port' in the select list", r.Errors![0].Message);
    }

    // ── Still allowed: sorting works or is moot ───────────────

    [Fact]
    public void OrderBy_ColumnInSelect_Sorts()
    {
        var r = _engine.ExecuteOne("get routes select host, port order by port desc", "testdb");

        Assert.Equal("c,b,a", Hosts(r));
    }

    [Fact]
    public void OrderBy_NoSelect_Sorts()
    {
        var r = _engine.ExecuteOne("get routes order by port desc", "testdb");

        Assert.Equal("c,b,a", Hosts(r));
    }

    [Fact]
    public void OrderBy_ExcludeSelectKeepsSortColumn_Sorts()
    {
        var r = _engine.ExecuteOne("get routes -select active order by port desc", "testdb");

        Assert.Equal("c,b,a", Hosts(r));
    }

    [Fact]
    public void OrderBy_IdWithLimit_TopNFastPath_Sorts()
    {
        // 'order by _id [desc] limit N' is served by the top-N path without _id selected.
        var r = _engine.ExecuteOne("get routes select host order by _id desc limit 3", "testdb");

        Assert.Equal("c,b,a", Hosts(r));
    }

    [Fact]
    public void OrderBy_IdWithCursorPaging_Allowed()
    {
        // 'after' orders by _id by construction; 'order by _id' is redundant but legal.
        var r = _engine.ExecuteOne("get routes select host order by _id after '0' limit 2", "testdb");

        Assert.Equal(SproutOperation.Get, r.Operation);
        Assert.Equal("a,b", Hosts(r));
    }

    [Fact]
    public void OrderBy_ComputedAlias_Sorts()
    {
        var r = _engine.ExecuteOne("get routes select host, port * 1 as p order by p desc", "testdb");

        Assert.Equal("c,b,a", Hosts(r));
    }

    [Fact]
    public void OrderBy_SelectAlias_Sorts()
    {
        // The output name is what lands on the row — sorting by the alias works.
        var r = _engine.ExecuteOne("get routes select host, port as p order by p desc", "testdb");

        Assert.Equal("c,b,a", Hosts(r));
    }

    [Fact]
    public void OrderBy_LiteralAlias_Allowed()
    {
        var r = _engine.ExecuteOne("get routes select host, 1 as v order by v", "testdb");

        Assert.Equal(SproutOperation.Get, r.Operation);
        Assert.Equal(3, r.Data!.Count);
    }

    [Fact]
    public void OrderBy_WithCount_NotChecked()
    {
        // count returns no rows — ordering is moot, rejecting it would break a harmless query.
        var r = _engine.ExecuteOne("get routes select host order by port count", "testdb");

        Assert.Equal(SproutOperation.Get, r.Operation);
        Assert.Equal(3, r.Affected);
    }

    [Fact]
    public void OrderBy_FollowDotColumn_Allowed()
    {
        _engine.ExecuteOne("create table backends (route_id ulong, name string 50)", "testdb");
        _engine.ExecuteOne("upsert backends {route_id: 1, name: 'x'}", "testdb");

        var r = _engine.ExecuteOne(
            "get routes select host follow routes._id -> backends.route_id as b order by b.name",
            "testdb");

        Assert.Equal(SproutOperation.Get, r.Operation);
    }
}

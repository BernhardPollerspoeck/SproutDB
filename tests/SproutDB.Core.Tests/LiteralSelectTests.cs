namespace SproutDB.Core.Tests;

/// <summary>
/// Executor-level tests for constant select columns (<c>true as preserve_host</c>).
/// Parser coverage lives in Parsing/LiteralSelectParserTests.
/// </summary>
public class LiteralSelectTests : IDisposable
{
    private readonly string _tempDir;
    private readonly SproutEngine _engine;

    public LiteralSelectTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), $"sproutdb-test-{Guid.NewGuid()}");
        _engine = new SproutEngine(_tempDir);
        _engine.ExecuteOne("create database", "testdb");

        _engine.ExecuteOne("create table routes (host string 50, port uint, active bool)", "testdb");
        _engine.ExecuteOne("upsert routes {host: 'a.example', port: 80, active: true}", "testdb");
        _engine.ExecuteOne("upsert routes {host: 'b.example', port: 443, active: false}", "testdb");
        _engine.ExecuteOne("upsert routes {host: 'c.example', port: 8080, active: true}", "testdb");
    }

    public void Dispose()
    {
        _engine.Dispose();
        if (Directory.Exists(_tempDir))
            Directory.Delete(_tempDir, true);
    }

    private List<Dictionary<string, object?>> Get(string query)
    {
        var r = _engine.ExecuteOne(query, "testdb");
        Assert.Equal(SproutOperation.Get, r.Operation);
        Assert.NotNull(r.Data);
        return r.Data;
    }

    // ── Values and types ──────────────────────────────────────

    [Fact]
    public void Literal_Bool_InEveryRow()
    {
        var rows = Get("get routes select host, true as preserve_host");

        Assert.Equal(3, rows.Count);
        Assert.All(rows, row => Assert.Equal(true, row["preserve_host"]));
    }

    [Fact]
    public void Literal_String_InEveryRow()
    {
        var rows = Get("get routes select host, 'auto' as backend_protocol");

        Assert.Equal(3, rows.Count);
        Assert.All(rows, row => Assert.Equal("auto", row["backend_protocol"]));
    }

    [Fact]
    public void Literal_Integer_KeepsLongType()
    {
        var rows = Get("get routes select host, 1 as version");

        Assert.All(rows, row => Assert.Equal(1L, row["version"]));
        Assert.IsType<long>(rows[0]["version"]);
    }

    [Fact]
    public void Literal_Float_KeepsDoubleType()
    {
        var rows = Get("get routes select host, 2.5 as factor");

        Assert.All(rows, row => Assert.Equal(2.5, row["factor"]));
        Assert.IsType<double>(rows[0]["factor"]);
    }

    [Fact]
    public void Literal_NegativeInteger_KeepsLongType()
    {
        var rows = Get("get routes select host, -1 as offset");

        Assert.All(rows, row => Assert.Equal(-1L, row["offset"]));
        Assert.IsType<long>(rows[0]["offset"]);
    }

    [Fact]
    public void Literal_Null_IsPresentAndNull()
    {
        var rows = Get("get routes select host, null as cert_path");

        Assert.Equal(3, rows.Count);
        Assert.All(rows, row =>
        {
            Assert.True(row.ContainsKey("cert_path"));
            Assert.Null(row["cert_path"]);
        });
    }

    [Fact]
    public void Literal_False()
    {
        var rows = Get("get routes select host, false as disabled");

        Assert.All(rows, row => Assert.Equal(false, row["disabled"]));
    }

    [Fact]
    public void Literal_Multiple_AllPresent()
    {
        var rows = Get("get routes select host, true as a, 'x' as b, 3 as c");

        Assert.All(rows, row =>
        {
            Assert.Equal(true, row["a"]);
            Assert.Equal("x", row["b"]);
            Assert.Equal(3L, row["c"]);
        });
    }

    [Fact]
    public void Literal_Only_NoRealColumn()
    {
        var rows = Get("get routes select 1 as x");

        Assert.Equal(3, rows.Count);
        Assert.All(rows, row =>
        {
            var kv = Assert.Single(row);
            Assert.Equal("x", kv.Key);
            Assert.Equal(1L, kv.Value);
        });
    }

    [Fact]
    public void Literal_DoesNotLeakIntoUnselectedColumns()
    {
        var rows = Get("get routes select host, 1 as x");

        Assert.All(rows, row =>
        {
            Assert.Equal(2, row.Count);
            Assert.True(row.ContainsKey("host"));
            Assert.True(row.ContainsKey("x"));
        });
    }

    // ── Key order ─────────────────────────────────────────────

    [Fact]
    public void Literal_KeyOrder_LiteralFirst()
    {
        var rows = Get("get routes select true as a, host");

        Assert.Equal(new[] { "a", "host" }, rows[0].Keys.ToArray());
    }

    [Fact]
    public void Literal_KeyOrder_LiteralInTheMiddle()
    {
        var rows = Get("get routes select host, 1 as mid, port");

        Assert.Equal(new[] { "host", "mid", "port" }, rows[0].Keys.ToArray());
    }

    [Fact]
    public void Literal_KeyOrder_LiteralLast()
    {
        var rows = Get("get routes select host, port, 1 as z");

        Assert.Equal(new[] { "host", "port", "z" }, rows[0].Keys.ToArray());
    }

    [Fact]
    public void Literal_KeyOrder_MultipleInterleaved()
    {
        var rows = Get("get routes select 1 as a, host, 2 as b, port, 3 as c");

        Assert.Equal(new[] { "a", "host", "b", "port", "c" }, rows[0].Keys.ToArray());
    }

    [Fact]
    public void Literal_KeyOrder_WithVirtualColumn()
    {
        var rows = Get("get routes select host, 1 as x, _id");

        Assert.Equal(new[] { "host", "x", "_id" }, rows[0].Keys.ToArray());
    }

    // ── Combination with other clauses ────────────────────────

    [Fact]
    public void Literal_WithWhere()
    {
        var rows = Get("get routes select host, true as flag where port = 443");

        var row = Assert.Single(rows);
        Assert.Equal("b.example", row["host"]);
        Assert.Equal(true, row["flag"]);
    }

    [Fact]
    public void Literal_EmptyResult_NoRows()
    {
        var rows = Get("get routes select host, 1 as x where port = 9999");

        Assert.Empty(rows);
    }

    [Fact]
    public void Literal_WithComputedColumn()
    {
        var rows = Get("get routes select host, port * 2 as double_port, 1 as v");

        Assert.All(rows, row =>
        {
            Assert.True(row.ContainsKey("double_port"));
            Assert.Equal(1L, row["v"]);
        });
    }

    [Fact]
    public void Literal_WithDistinct()
    {
        // The literal is constant, so it must not change how many rows survive distinct.
        var withLiteral = Get("get routes select active, 1 as v distinct");
        var withoutLiteral = Get("get routes select active distinct");

        Assert.Equal(withoutLiteral.Count, withLiteral.Count);
        Assert.All(withLiteral, row => Assert.Equal(1L, row["v"]));
    }

    [Fact]
    public void Literal_OrderByLiteralAlias_IsNoOpNotError()
    {
        var rows = Get("get routes select host, 1 as v order by v");

        Assert.Equal(3, rows.Count);
        Assert.All(rows, row => Assert.Equal(1L, row["v"]));
    }

    [Fact]
    public void Literal_WithOrderByRealColumn()
    {
        // 'port' has to be selected for the sort to see it — ordering happens on the
        // projected rows. That is pre-existing behaviour and not specific to literals.
        var rows = Get("get routes select host, port, 1 as v order by port desc");

        Assert.Equal(3, rows.Count);
        Assert.Equal("c.example", rows[0]["host"]);
        Assert.All(rows, row => Assert.Equal(1L, row["v"]));
    }

    [Fact]
    public void Literal_WithCount_DataStaysEmpty()
    {
        var r = _engine.ExecuteOne("get routes select host, 1 as v count", "testdb");

        Assert.Equal(3, r.Affected);
        Assert.NotNull(r.Data);
        Assert.Empty(r.Data);
    }

    [Fact]
    public void Literal_TopNFastPath()
    {
        // order by _id + limit takes the bounded-heap path, which projects separately.
        var rows = Get("get routes select host, 1 as v order by _id limit 2");

        Assert.Equal(2, rows.Count);
        Assert.All(rows, row => Assert.Equal(1L, row["v"]));
    }

    [Fact]
    public void Literal_TopNFastPath_Descending()
    {
        var rows = Get("get routes select host, 1 as v order by _id desc limit 2");

        Assert.Equal(2, rows.Count);
        Assert.All(rows, row => Assert.Equal(1L, row["v"]));
    }

    [Fact]
    public void Literal_CursorPaging()
    {
        // Only 'after' takes the cursor path (ExecuteCursor), which projects on its own.
        var r = _engine.ExecuteOne("get routes select host, 'p' as marker after '0' limit 2", "testdb");

        Assert.NotNull(r.Data);
        Assert.Equal(2, r.Data.Count);
        Assert.All(r.Data, row => Assert.Equal("p", row["marker"]));
    }

    [Fact]
    public void Literal_CursorPaging_FollowCursorToNextPage()
    {
        var first = _engine.ExecuteOne("get routes select host, 'p' as marker after '0' limit 2", "testdb");
        var cursor = first.Paging?.NextCursor;
        Assert.NotNull(cursor);

        var second = _engine.ExecuteOne(
            $"get routes select host, 'p' as marker after '{cursor}' limit 2", "testdb");

        Assert.NotNull(second.Data);
        Assert.NotEmpty(second.Data);
        Assert.All(second.Data, row => Assert.Equal("p", row["marker"]));
    }

    [Fact]
    public void Literal_CursorPaging_KeyOrderPreserved()
    {
        var r = _engine.ExecuteOne("get routes select 1 as a, host after '0' limit 1", "testdb");

        Assert.NotNull(r.Data);
        Assert.Equal(new[] { "a", "host" }, r.Data[0].Keys.ToArray());
    }

    [Fact]
    public void Literal_WithBTreeIndexLookup()
    {
        // An indexed equality goes through ReadRowsByPlaces instead of the full walk.
        _engine.ExecuteOne("create index routes.port", "testdb");

        var rows = Get("get routes select host, 1 as v where port = 80");

        var row = Assert.Single(rows);
        Assert.Equal("a.example", row["host"]);
        Assert.Equal(1L, row["v"]);
    }

    [Fact]
    public void Literal_WithPaging()
    {
        var rows = Get("get routes select host, 1 as v page 1 size 2");

        Assert.Equal(2, rows.Count);
        Assert.All(rows, row => Assert.Equal(1L, row["v"]));
    }

    // ── group by ──────────────────────────────────────────────

    [Fact]
    public void Literal_WithGroupByCount()
    {
        var rows = Get("get routes select 1 as v group by active count");

        Assert.Equal(2, rows.Count);
        Assert.All(rows, row => Assert.Equal(1L, row["v"]));
    }

    [Fact]
    public void Literal_WithAggregate_NotExpressible()
    {
        // The aggregate sits between table name and select ("get t sum port as total"), and
        // the parser skips the select clause entirely once it sees one. So a literal can
        // never reach the aggregate path — documented here rather than left to assumption.
        var r = _engine.ExecuteOne("get routes sum port as total group by active", "testdb");
        Assert.Equal(SproutOperation.Get, r.Operation);

        var withSelect = _engine.ExecuteOne("get routes select 1 as v sum port as total", "testdb");
        Assert.Equal(SproutOperation.Error, withSelect.Operation);
    }

    [Fact]
    public void Literal_WithGroupByNoAggregate()
    {
        var rows = Get("get routes select 1 as v group by active");

        Assert.Equal(2, rows.Count);
        Assert.All(rows, row => Assert.Equal(1L, row["v"]));
    }
}

/// <summary>
/// Literal behaviour across the four join types — in particular that a constant stays
/// constant on rows a right/outer join builds without a source.
/// </summary>
public class LiteralSelectFollowTests : IDisposable
{
    private readonly string _tempDir;
    private readonly SproutEngine _engine;

    public LiteralSelectFollowTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), $"sproutdb-test-{Guid.NewGuid()}");
        _engine = new SproutEngine(_tempDir);
        _engine.ExecuteOne("create database", "testdb");

        _engine.ExecuteOne("create table routes (host string 50)", "testdb");
        _engine.ExecuteOne("create table backends (route_id ulong, name string 50)", "testdb");

        // routes 1 and 2 have backends, route 3 has none.
        _engine.ExecuteOne("upsert routes {host: 'a.example'}", "testdb");
        _engine.ExecuteOne("upsert routes {host: 'b.example'}", "testdb");
        _engine.ExecuteOne("upsert routes {host: 'orphan.example'}", "testdb");

        _engine.ExecuteOne("upsert backends {route_id: 1, name: 'be-1'}", "testdb");
        _engine.ExecuteOne("upsert backends {route_id: 2, name: 'be-2'}", "testdb");
        // A backend pointing at no route at all — surfaces only in right/outer joins.
        _engine.ExecuteOne("upsert backends {route_id: 99, name: 'be-orphan'}", "testdb");
    }

    public void Dispose()
    {
        _engine.Dispose();
        if (Directory.Exists(_tempDir))
            Directory.Delete(_tempDir, true);
    }

    private List<Dictionary<string, object?>> Get(string query)
    {
        var r = _engine.ExecuteOne(query, "testdb");
        Assert.Equal(SproutOperation.Get, r.Operation);
        Assert.NotNull(r.Data);
        return r.Data;
    }

    [Fact]
    public void Literal_InnerJoin_ConstantEverywhere()
    {
        var rows = Get("get routes select host, 1 as v follow routes._id -> backends.route_id as b");

        Assert.Equal(2, rows.Count);
        Assert.All(rows, row => Assert.Equal(1L, row["v"]));
    }

    [Fact]
    public void Literal_LeftJoin_ConstantOnUnmatchedSourceRows()
    {
        var rows = Get("get routes select host, 1 as v follow routes._id ->? backends.route_id as b");

        Assert.Equal(3, rows.Count);
        Assert.All(rows, row => Assert.Equal(1L, row["v"]));
    }

    [Fact]
    public void Literal_RightJoin_ConstantOnUnmatchedTargetRows()
    {
        // The bug this guards: the right/outer path nulls every source key on rows built
        // from an unmatched target. A literal is not a source value and must survive.
        var rows = Get("get routes select host, 1 as v follow routes._id ?-> backends.route_id as b");

        Assert.Equal(3, rows.Count);
        Assert.All(rows, row => Assert.Equal(1L, row["v"]));

        // The orphan backend row has no source: host is null, but v is still 1.
        var orphan = Assert.Single(rows, row => row["host"] is null);
        Assert.Equal(1L, orphan["v"]);
    }

    [Fact]
    public void Literal_OuterJoin_ConstantOnBothUnmatchedSides()
    {
        var rows = Get("get routes select host, 1 as v follow routes._id ?->? backends.route_id as b");

        Assert.Equal(4, rows.Count);
        Assert.All(rows, row => Assert.Equal(1L, row["v"]));
        Assert.Contains(rows, row => row["host"] is null);            // unmatched backend
        Assert.Contains(rows, row => Equals(row["host"], "orphan.example")); // unmatched route
    }

    [Fact]
    public void Literal_RightJoin_StringLiteralSurvives()
    {
        var rows = Get("get routes select host, 'k' as marker follow routes._id ?-> backends.route_id as b");

        Assert.All(rows, row => Assert.Equal("k", row["marker"]));
    }

    [Fact]
    public void Literal_RightJoin_NullLiteralStaysNull()
    {
        var rows = Get("get routes select host, null as n follow routes._id ?-> backends.route_id as b");

        Assert.All(rows, row =>
        {
            Assert.True(row.ContainsKey("n"));
            Assert.Null(row["n"]);
        });
    }

    // ── Post-follow select ────────────────────────────────────

    [Fact]
    public void Literal_PostFollowSelect()
    {
        var rows = Get(
            "get routes follow routes._id -> backends.route_id as b select host, b.name, true as ha");

        Assert.Equal(2, rows.Count);
        Assert.All(rows, row => Assert.Equal(true, row["ha"]));
    }

    [Fact]
    public void Literal_PostFollowSelect_WithoutDotColumn()
    {
        // The literal alone marks this as a post-follow select.
        var rows = Get(
            "get routes follow routes._id -> backends.route_id as b select host, true as ha");

        Assert.Equal(2, rows.Count);
        Assert.All(rows, row =>
        {
            Assert.Equal(true, row["ha"]);
            Assert.True(row.ContainsKey("host"));
        });
    }

    [Fact]
    public void Literal_PostFollowSelect_KeyOrder()
    {
        var rows = Get(
            "get routes follow routes._id -> backends.route_id as b select 1 as first, host, b.name");

        Assert.Equal(new[] { "first", "host", "b.name" }, rows[0].Keys.ToArray());
    }

    [Fact]
    public void Literal_BaseLiteral_DroppedByExplicitPostFollowSelect()
    {
        // Matches how base computed columns behave: an explicit post-follow select keeps
        // only what it lists. Needs b.name — without a dot/literal/operator the select
        // would be read as a follow-level select instead.
        var rows = Get(
            "get routes select host, 1 as v follow routes._id -> backends.route_id as b select host, b.name");

        Assert.Equal(2, rows.Count);
        Assert.All(rows, row => Assert.False(row.ContainsKey("v")));
    }

    [Fact]
    public void Literal_BaseLiteral_KeptWhenListedInPostFollowSelect()
    {
        var rows = Get(
            "get routes select host, 1 as v follow routes._id -> backends.route_id as b select host, b.name, v");

        Assert.Equal(2, rows.Count);
        Assert.All(rows, row => Assert.Equal(1L, row["v"]));
    }

    [Fact]
    public void Literal_BaseAndPostFollowLiterals_Coexist()
    {
        var rows = Get(
            "get routes select host, 1 as v follow routes._id -> backends.route_id as b select host, v, 2 as w");

        Assert.All(rows, row =>
        {
            Assert.Equal(1L, row["v"]);
            Assert.Equal(2L, row["w"]);
        });
    }

    [Fact]
    public void Literal_PostFollow_OrderByLiteralAlias_IsNoOpNotError()
    {
        // The post-follow literal alias must reach the order-by whitelist, otherwise this
        // is rejected as "column does not exist" even though the key is in the result.
        var rows = Get(
            "get routes follow routes._id -> backends.route_id as b select host, 7 as v order by v");

        Assert.Equal(2, rows.Count);
        Assert.All(rows, row => Assert.Equal(7L, row["v"]));
    }

    [Fact]
    public void Literal_PostFollowSelect_LeftJoin()
    {
        var rows = Get(
            "get routes follow routes._id ->? backends.route_id as b select host, 9 as v");

        Assert.Equal(3, rows.Count);
        Assert.All(rows, row => Assert.Equal(9L, row["v"]));
    }
}

namespace SproutDB.Core.Tests;

/// <summary>
/// Parameterized queries: <c>@name</c> placeholders bound to values that can
/// never become query syntax.
/// </summary>
public class ParameterTests : IDisposable
{
    private readonly string _tempDir;
    private readonly SproutEngine _engine;
    private readonly ISproutDatabase _db;

    public ParameterTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), $"sproutdb-test-{Guid.NewGuid()}");
        _engine = new SproutEngine(_tempDir);
        _db = _engine.GetOrCreateDatabase("testdb");
        _db.Query("create table gs (k string 64 unique, etag string 32, n slong, f double, ok bool, at datetime, data blob)");
    }

    public void Dispose()
    {
        _engine.Dispose();
        if (Directory.Exists(_tempDir))
            Directory.Delete(_tempDir, true);
    }

    private SproutResponse One(string query, object parameters) => _db.Query(query, parameters)[0];

    private static void AssertParameterError(SproutResponse r, string messagePart)
    {
        Assert.Equal(SproutOperation.Error, r.Operation);
        var error = Assert.Single(r.Errors ?? []);
        Assert.Equal("PARAMETER_ERROR", error.Code);
        Assert.Contains(messagePart, error.Message);
    }

    // ── Binding values ───────────────────────────────────────

    [Fact]
    public void Where_WithParameter_FindsRow()
    {
        _db.Query("upsert gs {k: 'a', etag: 'E1'}");

        var r = One("get gs select k, etag where k = @key", new { key = "a" });

        Assert.Equal("E1", Assert.Single(r.Data ?? [])["etag"]);
    }

    [Fact]
    public void Upsert_WithTypedParameters_StoresValues()
    {
        var at = new DateTime(2026, 9, 11, 12, 30, 15);
        var r = One("upsert gs {k: @k, etag: @etag, n: @n, f: @f, ok: @ok, at: @at, data: @data}",
            new { k = "a", etag = (string?)null, n = -42L, f = 1.5, ok = true, at, data = new byte[] { 1, 2, 3 } });
        Assert.Null(r.Errors);

        var row = Assert.Single(_db.Query("get gs")[0].Data ?? []);
        Assert.Null(row["etag"]);
        Assert.Equal(-42L, row["n"]);
        Assert.Equal(1.5, row["f"]);
        Assert.Equal(true, row["ok"]);
        Assert.Equal("2026-09-11 12:30:15", row["at"]?.ToString());
        Assert.Equal(Convert.ToBase64String([1, 2, 3]), row["data"]);
    }

    [Fact]
    public void InList_WithListParameter()
    {
        _db.Query("upsert gs [{k: 'a'}, {k: 'b'}, {k: 'c'}]");

        var r = One("get gs select k where k in @keys order by k", new { keys = new[] { "a", "c" } });

        Assert.Equal(["a", "c"], (r.Data ?? []).Select(row => row["k"] as string));
    }

    [Fact]
    public void ConditionalUpsert_WithParameters()
    {
        _db.Query("upsert gs {k: 'a', etag: 'E1'}");

        var ok = One("upsert gs {k: @k, etag: @next} on k when etag = @expected", new { k = "a", next = "E2", expected = "E1" });
        Assert.Null(ok.Errors);

        var stale = One("upsert gs {k: @k, etag: @next} on k when etag = @expected", new { k = "a", next = "E3", expected = "E1" });
        Assert.Equal("CONDITION_FAILED", stale.Errors?[0].Code);
    }

    [Fact]
    public void SameParameterUsedTwice()
    {
        var r = One("upsert gs {k: @v, etag: @v}", new { v = "same" });

        Assert.Null(r.Errors);
        Assert.Equal("same", Assert.Single(_db.Query("get gs")[0].Data ?? [])["etag"]);
    }

    [Fact]
    public void ParameterNames_AreCaseInsensitive()
    {
        var r = One("upsert gs {k: @Key}", new { key = "a" });
        Assert.Null(r.Errors);
    }

    [Fact]
    public void DictionaryParameters()
    {
        var typed = new Dictionary<string, object?> { ["key"] = "a" };
        Assert.Null(_db.Query("upsert gs {k: @key}", typed)[0].Errors);

        var plain = new Dictionary<string, string> { ["key"] = "b" };
        Assert.Null(_db.Query("upsert gs {k: @key}", plain)[0].Errors);

        Assert.Equal(2, _db.Query("get gs")[0].Data?.Count);
    }

    // ── Injection ────────────────────────────────────────────

    [Theory]
    [InlineData("a' or k != '")]
    [InlineData("x'; purge table gs; ##")]
    [InlineData("a\\")]
    [InlineData("\\'; purge table gs; ##")]
    [InlineData("@key")]
    [InlineData("## comment")]
    public void HostileValues_StayValues(string hostile)
    {
        _db.Query("upsert gs {k: 'a'}");

        var insert = One("upsert gs {k: @k}", new { k = hostile });
        Assert.Null(insert.Errors);

        var byKey = One("get gs select k where k = @k", new { k = hostile });
        Assert.Equal(hostile, Assert.Single(byKey.Data ?? [])["k"]);

        // The table is intact and holds exactly the two rows
        Assert.Equal(2, _db.Query("get gs")[0].Data?.Count);
    }

    [Fact]
    public void ParameterAtNamePosition_IsNotExecutedAsName()
    {
        var r = One("get @table", new { table = "gs" });

        Assert.Equal(SproutOperation.Error, r.Operation);
        Assert.Equal("SYNTAX_ERROR", r.Errors?[0].Code);
    }

    [Fact]
    public void AtSignInsideStringLiteral_IsNoParameter()
    {
        var r = _db.Query("upsert gs {k: 'mail@example.com'}")[0];

        Assert.Null(r.Errors);
        Assert.Equal("mail@example.com", Assert.Single(_db.Query("get gs")[0].Data ?? [])["k"]);
    }

    // ── Errors ───────────────────────────────────────────────

    [Fact]
    public void MissingParameter_IsParameterErrorWithPosition()
    {
        const string query = "get gs where k = @key";
        var r = One(query, new { other = "x" });

        var missing = Assert.Single(r.Errors ?? [], e => e.Message.Contains("missing"));
        Assert.Equal("PARAMETER_ERROR", missing.Code);
        Assert.Equal(query.IndexOf("@key", StringComparison.Ordinal), missing.Position);
    }

    [Fact]
    public void UnusedParameter_IsParameterError()
    {
        AssertParameterError(One("get gs where k = @key", new { key = "a", extra = 1 }), "unused parameter '@extra'");
    }

    [Fact]
    public void PlaceholderWithoutParameters_IsParameterError()
    {
        AssertParameterError(_db.Query("get gs where k = @key")[0], "missing parameter '@key'");
    }

    [Fact]
    public void DuplicateNameDifferentCase_IsParameterError()
    {
        var parameters = new Dictionary<string, object?> { ["key"] = "a", ["KEY"] = "b" };
        AssertParameterError(_db.Query("get gs where k = @key", parameters)[0], "more than once");
    }

    [Theory]
    [InlineData(double.NaN)]
    [InlineData(double.PositiveInfinity)]
    public void NaNOrInfinity_IsParameterError(double value)
    {
        AssertParameterError(One("upsert gs {k: 'a', f: @f}", new { f = value }), "NaN and infinity");
    }

    [Fact]
    public void NestedList_IsParameterError()
    {
        AssertParameterError(One("get gs where k in @keys", new { keys = new[] { new[] { "a" } } }), "single values");
    }

    [Fact]
    public void ParameterError_ExecutesNothing()
    {
        var results = _db.Query("upsert gs {k: 'a'}; upsert gs {k: @missing}");

        Assert.Single(results);
        Assert.Empty(_db.Query("get gs")[0].Data ?? [new Dictionary<string, object?>()]);
    }
}

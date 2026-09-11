namespace SproutDB.Core.Tests;

/// <summary>
/// <c>unique</c> column modifier in <c>create table</c>, and the ban on schema
/// changes inside <c>atomic</c>.
/// </summary>
public class UniqueColumnTests : IDisposable
{
    private readonly string _tempDir;
    private SproutEngine _engine;

    public UniqueColumnTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), $"sproutdb-test-{Guid.NewGuid()}");
        _engine = new SproutEngine(_tempDir);
        _engine.ExecuteOne("create database", "testdb");
    }

    public void Dispose()
    {
        _engine.Dispose();
        if (Directory.Exists(_tempDir))
            Directory.Delete(_tempDir, true);
    }

    private SproutResponse Run(string query) => _engine.ExecuteOne(query, "testdb");

    [Fact]
    public void CreateTable_UniqueColumn_IsEnforcedImmediately()
    {
        var create = Run("create table gs (k string 64 strict unique, etag string 16)");
        Assert.Null(create.Errors);
        var createdCol = create.Schema?.Columns.Single(c => c.Name == "k");
        Assert.True(createdCol?.IsUnique);

        Assert.Null(Run("upsert gs {k: 'a', etag: 'E1'}").Errors);
        Assert.Equal("UNIQUE_VIOLATION", Run("upsert gs {k: 'a', etag: 'E2'}").Errors?[0].Code);

        var describe = Run("describe gs");
        var col = describe.Schema?.Columns.Single(c => c.Name == "k");
        Assert.True(col?.IsUnique);
        Assert.True(col?.Indexed);
    }

    [Theory]
    [InlineData("k string 64 unique")]
    [InlineData("k string 64 unique strict")]
    [InlineData("k string 64 strict unique")]
    [InlineData("k string 64 unique default 'x'")]
    [InlineData("k sint default 0 strict unique")]
    public void CreateTable_UniqueModifier_AnyOrder(string columnDef)
    {
        var r = Run($"create table t ({columnDef})");

        Assert.Null(r.Errors);
        Assert.True(Run("describe t").Schema?.Columns.Single(c => c.Name == "k").IsUnique);
    }

    [Fact]
    public void CreateTable_UniqueColumn_AllowsMultipleNulls()
    {
        Run("create table t (k string 10 unique, v sint)");

        Assert.Null(Run("upsert t {v: 1}").Errors);
        Assert.Null(Run("upsert t {k: null, v: 2}").Errors);
    }

    [Theory]
    [InlineData("data blob unique")]
    [InlineData("tags array string 10 unique")]
    public void CreateTable_UniqueOnBlobOrArray_IsRejected(string columnDef)
    {
        var r = Run($"create table t ({columnDef})");

        Assert.Equal("TYPE_MISMATCH", r.Errors?[0].Code);
        Assert.Equal("UNKNOWN_TABLE", Run("describe t").Errors?[0].Code);
    }

    [Fact]
    public void CreateTable_UniqueColumn_SurvivesRestart()
    {
        Run("create table gs (k string 64 unique)");
        Run("upsert gs {k: 'a'}");

        _engine.Dispose();
        _engine = new SproutEngine(_tempDir);

        Assert.Equal("UNIQUE_VIOLATION", Run("upsert gs {k: 'a'}").Errors?[0].Code);
    }

    [Fact]
    public void UpsertOn_UniqueColumn_FromCreateTable_Works()
    {
        Run("create table gs (k string 64 unique, etag string 16)");
        Run("upsert gs {k: 'a', etag: 'E1'}");

        var r = Run("upsert gs {k: 'a', etag: 'E2'} on k when etag = 'E1'");

        Assert.Null(r.Errors);
        Assert.Equal((ulong)1, r.Data?[0]["_id"]);
    }

    // ── No schema changes inside atomic ──────────────────────

    [Theory]
    [InlineData("create table t (k string 10)")]
    [InlineData("create index unique t.k")]
    [InlineData("add column t.v sint")]
    [InlineData("purge table t")]
    public void Atomic_WithSchemaChange_IsSyntaxError_AndRunsNothing(string ddl)
    {
        Run("create table other (v sint)");

        var results = _engine.Execute($"atomic; upsert other {{v: 1}}; {ddl}; commit", "testdb");

        var error = Assert.Single(results);
        Assert.Equal("SYNTAX_ERROR", error.Errors?[0].Code);
        Assert.Contains("only upsert, delete, get and describe", error.Errors?[0].Message ?? "");
        Assert.Empty(Run("get other").Data ?? [new Dictionary<string, object?>()]);
    }
}

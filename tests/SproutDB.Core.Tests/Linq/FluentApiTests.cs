using SproutDB.Core.Linq;

namespace SproutDB.Core.Tests.Linq;

public class FluentApiTests : IDisposable
{
    private readonly string _tempDir;
    private readonly SproutEngine _engine;
    private readonly ISproutDatabase _db;

    public FluentApiTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), $"sproutdb-fluent-{Guid.NewGuid()}");
        _engine = new SproutEngine(_tempDir);
        _db = _engine.GetOrCreateDatabase("testdb");
    }

    public void Dispose()
    {
        _engine.Dispose();
        if (Directory.Exists(_tempDir))
            Directory.Delete(_tempDir, true);
    }

    // ── FluentTypeMapper ────────────────────────────────────

    [Theory]
    [InlineData(typeof(string), "string")]
    [InlineData(typeof(int), "sint")]
    [InlineData(typeof(long), "slong")]
    [InlineData(typeof(byte), "ubyte")]
    [InlineData(typeof(bool), "bool")]
    [InlineData(typeof(DateTime), "datetime")]
    [InlineData(typeof(DateOnly), "date")]
    [InlineData(typeof(TimeOnly), "time")]
    [InlineData(typeof(float), "float")]
    [InlineData(typeof(double), "double")]
    [InlineData(typeof(decimal), "double")]
    [InlineData(typeof(short), "sshort")]
    [InlineData(typeof(ushort), "ushort")]
    [InlineData(typeof(uint), "uint")]
    [InlineData(typeof(ulong), "ulong")]
    [InlineData(typeof(sbyte), "sbyte")]
    internal void FluentTypeMapper_MapsCorrectly(Type clrType, string expected)
    {
        Assert.Equal(expected, FluentTypeMapper.GetTypeName(clrType));
    }

    [Fact]
    internal void FluentTypeMapper_UnsupportedType_Throws()
    {
        Assert.Throws<ArgumentException>(() => FluentTypeMapper.GetTypeName(typeof(Guid)));
    }

    // ── CreateTableBuilder ──────────────────────────────────

    [Fact]
    public void CreateTable_BuildsCorrectQuery()
    {
        var builder = new CreateTableBuilder(_db, "users");
        builder.AddColumn<string>("name", 100)
               .AddColumn<int>("age")
               .AddColumn<bool>("active", defaultValue: "true");

        var query = builder.BuildQuery();
        Assert.Equal("create table users (name string 100, age sint, active bool default true)", query);
    }

    [Fact]
    public void CreateTable_StrictColumn()
    {
        var builder = new CreateTableBuilder(_db, "items");
        builder.AddColumn<string>("code", 50, strict: true);

        var query = builder.BuildQuery();
        Assert.Equal("create table items (code string 50 strict)", query);
    }

    [Fact]
    public void CreateTable_NoColumns()
    {
        var builder = new CreateTableBuilder(_db, "empty");

        var query = builder.BuildQuery();
        Assert.Equal("create table empty", query);
    }

    [Fact]
    public void CreateTable_StringWithoutSize_Throws()
    {
        var builder = new CreateTableBuilder(_db, "users");
        Assert.Throws<ArgumentException>(() => builder.AddColumn<string>("name"));
    }

    [Fact]
    public void CreateTable_Execute_Success()
    {
        var result = _db.CreateTable("users")
            .AddColumn<string>("name", 100)
            .AddColumn<int>("age")
            .Execute();

        Assert.Equal(SproutOperation.CreateTable, result.Operation);
        Assert.NotNull(result.Schema);
        Assert.Equal("users", result.Schema.Table);
    }

    [Fact]
    public void CreateTable_Execute_Duplicate_Throws()
    {
        _db.CreateTable("users")
            .AddColumn<string>("name", 100)
            .Execute();

        Assert.Throws<SproutQueryException>(() =>
            _db.CreateTable("users")
                .AddColumn<string>("name", 100)
                .Execute());
    }

    // ── AddColumn ───────────────────────────────────────────

    [Fact]
    public void AddColumn_Success()
    {
        _db.CreateTable("users")
            .AddColumn<string>("name", 100)
            .Execute();

        var result = _db.AddColumn<bool>("users", "active", defaultValue: "false");
        Assert.Equal(SproutOperation.AddColumn, result.Operation);
    }

    [Fact]
    public void AddColumn_StringWithoutSize_Throws()
    {
        _db.CreateTable("users")
            .AddColumn<string>("name", 100)
            .Execute();

        Assert.Throws<ArgumentException>(() =>
            _db.AddColumn<string>("users", "bio"));
    }

    // ── AlterColumn ─────────────────────────────────────────

    [Fact]
    public void AlterColumn_Success()
    {
        _db.CreateTable("users")
            .AddColumn<string>("name", 100)
            .Execute();

        var result = _db.AlterColumn("users", "name", 500);
        Assert.Equal(SproutOperation.AlterColumn, result.Operation);
    }
}

namespace SproutDB.Core.Tests;

public class SproutEngineTests : IDisposable
{
    private readonly string _tempDir;
    private readonly SproutEngine _engine;

    public SproutEngineTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), $"sproutdb-test-{Guid.NewGuid()}");
        _engine = new SproutEngine(_tempDir);
    }

    public void Dispose()
    {
        _engine.Dispose();
        if (Directory.Exists(_tempDir))
            Directory.Delete(_tempDir, true);
    }

    // ── create database ──────────────────────────────────────

    [Fact]
    public void CreateDatabase_Success()
    {
        var response = _engine.Execute("create database", "shop");

        Assert.Equal(SproutOperation.CreateDatabase, response.Operation);
        Assert.NotNull(response.Schema);
        Assert.Equal("shop", response.Schema.Database);
        Assert.Null(response.Errors);
        Assert.Null(response.AnnotatedQuery);
    }

    [Fact]
    public void CreateDatabase_CreatesDirectory()
    {
        _engine.Execute("create database", "shop");

        Assert.True(Directory.Exists(Path.Combine(_tempDir, "shop")));
    }

    [Fact]
    public void CreateDatabase_WritesMetaBin()
    {
        _engine.Execute("create database", "shop");

        var metaPath = Path.Combine(_tempDir, "shop", "_meta.bin");
        Assert.True(File.Exists(metaPath));
        Assert.Equal(8, new FileInfo(metaPath).Length); // 8 bytes = created_ticks (long)

        using var fs = new FileStream(metaPath, FileMode.Open, FileAccess.Read);
        using var br = new BinaryReader(fs);
        var ticks = br.ReadInt64();
        Assert.True(ticks > 0);
    }

    [Fact]
    public void CreateDatabase_NameIsLowercased()
    {
        _engine.Execute("create database", "MyShop");

        var dbPath = Path.Combine(_tempDir, "myshop");
        Assert.True(Directory.Exists(dbPath));
        // Verify actual directory name on disk is lowercase
        var actualDirName = new DirectoryInfo(dbPath).Name;
        Assert.Equal("myshop", actualDirName);
    }

    [Fact]
    public void CreateDatabase_ResponseSchemaHasLowercaseName()
    {
        var response = _engine.Execute("create database", "MyShop");

        Assert.Equal("myshop", response.Schema!.Database);
    }

    [Fact]
    public void CreateDatabase_AlreadyExists_Error()
    {
        _engine.Execute("create database", "shop");
        var response = _engine.Execute("create database", "shop");

        Assert.Equal(SproutOperation.Error, response.Operation);
        Assert.NotNull(response.Errors);
        Assert.Single(response.Errors);
        Assert.Equal("DATABASE_EXISTS", response.Errors[0].Code);
        Assert.Contains("shop", response.Errors[0].Message);
    }

    [Fact]
    public void CreateDatabase_AlreadyExists_AnnotatedQuery()
    {
        _engine.Execute("create database", "shop");
        var response = _engine.Execute("create database", "shop");

        Assert.NotNull(response.AnnotatedQuery);
        Assert.Contains("##", response.AnnotatedQuery);
        Assert.Contains("already exists", response.AnnotatedQuery);
    }

    // ── Name validation ──────────────────────────────────────

    [Theory]
    [InlineData("shop")]
    [InlineData("mydb")]
    [InlineData("db1")]
    [InlineData("a")]
    public void ValidName_Accepted(string name)
    {
        var response = _engine.Execute("create database", name);
        Assert.Equal(SproutOperation.CreateDatabase, response.Operation);
    }

    [Theory]
    [InlineData("")]
    [InlineData("1shop")]
    [InlineData("_system")]
    [InlineData("my-shop")]
    [InlineData("my_shop")]
    [InlineData("my.shop")]
    [InlineData("123")]
    public void InvalidName_Error(string name)
    {
        var response = _engine.Execute("create database", name);

        Assert.Equal(SproutOperation.Error, response.Operation);
        Assert.Equal("SYNTAX_ERROR", response.Errors![0].Code);
        Assert.Contains("invalid database name", response.Errors[0].Message);
    }

    // ── create table ────────────────────────────────────────

    [Fact]
    public void CreateTable_Success()
    {
        _engine.Execute("create database", "shop");
        var response = _engine.Execute("create table users (name string, age ubyte)", "shop");

        Assert.Equal(SproutOperation.CreateTable, response.Operation);
        Assert.NotNull(response.Schema);
        Assert.Equal("users", response.Schema.Table);
        Assert.Null(response.Errors);
    }

    [Fact]
    public void CreateTable_CreatesDirectory()
    {
        _engine.Execute("create database", "shop");
        _engine.Execute("create table users (name string)", "shop");

        Assert.True(Directory.Exists(Path.Combine(_tempDir, "shop", "users")));
    }

    [Fact]
    public void CreateTable_WritesSchemaBin()
    {
        _engine.Execute("create database", "shop");
        _engine.Execute("create table users (name string 100, age ubyte strict, active bool default true)", "shop");

        var schemaPath = Path.Combine(_tempDir, "shop", "users", "_schema.bin");
        Assert.True(File.Exists(schemaPath));
        Assert.True(new FileInfo(schemaPath).Length > 0);

        // Verify next_id is in _index header (slot 0)
        var indexPath = Path.Combine(_tempDir, "shop", "users", "_index");
        using var fs = new FileStream(indexPath, FileMode.Open, FileAccess.Read);
        using var br = new BinaryReader(fs);
        var nextId = br.ReadUInt64();
        Assert.Equal((ulong)1, nextId);
    }

    [Fact]
    public void CreateTable_CreatesIndexFile()
    {
        _engine.Execute("create database", "shop");
        _engine.Execute("create table users (name string)", "shop");

        var indexPath = Path.Combine(_tempDir, "shop", "users", "_index");
        Assert.True(File.Exists(indexPath));

        var expectedSize = (long)(10_000 + 1) * sizeof(long); // CHUNK_SIZE+1 entries * 8 bytes
        Assert.Equal(expectedSize, new FileInfo(indexPath).Length);
    }

    [Fact]
    public void CreateTable_CreatesColumnFiles()
    {
        _engine.Execute("create database", "shop");
        _engine.Execute("create table users (name string 100, age ubyte)", "shop");

        var tablePath = Path.Combine(_tempDir, "shop", "users");

        var nameCol = Path.Combine(tablePath, "name.col");
        Assert.True(File.Exists(nameCol));
        Assert.Equal((long)10_000 * 101, new FileInfo(nameCol).Length); // CHUNK_SIZE * (1 flag + 100)

        var ageCol = Path.Combine(tablePath, "age.col");
        Assert.True(File.Exists(ageCol));
        Assert.Equal((long)10_000 * 2, new FileInfo(ageCol).Length); // CHUNK_SIZE * (1 flag + 1)
    }

    [Fact]
    public void CreateTable_EmptyTable_NoColumnFiles()
    {
        _engine.Execute("create database", "shop");
        _engine.Execute("create table users", "shop");

        var tablePath = Path.Combine(_tempDir, "shop", "users");
        Assert.True(Directory.Exists(tablePath));
        Assert.True(File.Exists(Path.Combine(tablePath, "_schema.bin")));
        Assert.True(File.Exists(Path.Combine(tablePath, "_index")));

        // No .col files
        Assert.Empty(Directory.GetFiles(tablePath, "*.col"));
    }

    [Fact]
    public void CreateTable_ResponseIncludesIdColumn()
    {
        _engine.Execute("create database", "shop");
        var response = _engine.Execute("create table users (name string)", "shop");

        var columns = response.Schema!.Columns!;
        Assert.Equal(2, columns.Count); // id + name

        Assert.Equal("id", columns[0].Name);
        Assert.Equal("ulong", columns[0].Type);
        Assert.False(columns[0].Nullable);
        Assert.True(columns[0].Strict);
        Assert.True(columns[0].Auto);

        Assert.Equal("name", columns[1].Name);
        Assert.Equal("string", columns[1].Type);
    }

    [Fact]
    public void CreateTable_ResponseStringSizeIncluded()
    {
        _engine.Execute("create database", "shop");
        var response = _engine.Execute("create table users (name string 500, age ubyte)", "shop");

        var columns = response.Schema!.Columns!;
        Assert.Equal(500, columns[1].Size); // name string has size
        Assert.Null(columns[2].Size);       // age ubyte has no size in response
    }

    [Fact]
    public void CreateTable_AlreadyExists_Error()
    {
        _engine.Execute("create database", "shop");
        _engine.Execute("create table users (name string)", "shop");
        var response = _engine.Execute("create table users (name string)", "shop");

        Assert.Equal(SproutOperation.Error, response.Operation);
        Assert.Equal("TABLE_EXISTS", response.Errors![0].Code);
        Assert.Contains("users", response.Errors[0].Message);
    }

    [Fact]
    public void CreateTable_DatabaseNotFound_Error()
    {
        var response = _engine.Execute("create table users (name string)", "missing");

        Assert.Equal(SproutOperation.Error, response.Operation);
        Assert.Equal("UNKNOWN_DATABASE", response.Errors![0].Code);
        Assert.Contains("missing", response.Errors[0].Message);
    }

    [Fact]
    public void CreateTable_NameIsLowercased()
    {
        _engine.Execute("create database", "shop");
        var response = _engine.Execute("create table Users (Name STRING)", "shop");

        Assert.Equal("users", response.Schema!.Table);
    }

    // ── Parse errors propagate ───────────────────────────────

    [Fact]
    public void ParseError_Propagated()
    {
        var response = _engine.Execute("creat database", "shop");

        Assert.Equal(SproutOperation.Error, response.Operation);
        Assert.NotNull(response.Errors);
        Assert.Equal("SYNTAX_ERROR", response.Errors[0].Code);
        Assert.NotNull(response.AnnotatedQuery);
    }

    // ── End-to-end ───────────────────────────────────────────

    [Fact]
    public void EndToEnd_CreateDatabase_CaseInsensitive()
    {
        var response = _engine.Execute("CREATE DATABASE", "Shop");

        Assert.Equal(SproutOperation.CreateDatabase, response.Operation);
        Assert.Equal("shop", response.Schema!.Database);
        Assert.True(Directory.Exists(Path.Combine(_tempDir, "shop")));
    }

    [Fact]
    public void EndToEnd_CreateDatabase_WithComment()
    {
        var response = _engine.Execute("create database ## new db", "shop");

        Assert.Equal(SproutOperation.CreateDatabase, response.Operation);
    }

    [Fact]
    public void EndToEnd_MultipleDatabases()
    {
        var r1 = _engine.Execute("create database", "shop");
        var r2 = _engine.Execute("create database", "analytics");

        Assert.Equal(SproutOperation.CreateDatabase, r1.Operation);
        Assert.Equal(SproutOperation.CreateDatabase, r2.Operation);
        Assert.True(Directory.Exists(Path.Combine(_tempDir, "shop")));
        Assert.True(Directory.Exists(Path.Combine(_tempDir, "analytics")));
    }
}

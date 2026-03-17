namespace SproutDB.Core.Tests;

public class WriteProtectionTests : IDisposable
{
    private readonly string _tempDir;
    private readonly SproutEngine _engine;

    public WriteProtectionTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), $"sproutdb-test-{Guid.NewGuid()}");
        _engine = new SproutEngine(_tempDir);
        _engine.ExecuteOne("create database", "shop");
        _engine.ExecuteOne("create table users (name string 100, age ubyte)", "shop");
        _engine.ExecuteOne("upsert users {name: 'Alice', age: 25}", "shop");
    }

    public void Dispose()
    {
        _engine.Dispose();
        if (Directory.Exists(_tempDir))
            Directory.Delete(_tempDir, true);
    }

    // ── Database-level protection ─────────────────────────────

    [Fact]
    public void CreateDatabase_Protected_Error()
    {
        var r = _engine.ExecuteOne("create database", "_foo");

        Assert.Equal(SproutOperation.Error, r.Operation);
        Assert.Equal("PROTECTED_NAME", r.Errors?[0].Code);
        Assert.Contains("_foo", r.Errors?[0].Message ?? "");
    }

    [Fact]
    public void PurgeDatabase_Protected_Error()
    {
        var r = _engine.ExecuteOne("purge database", "_system");

        Assert.Equal(SproutOperation.Error, r.Operation);
        Assert.Equal("PROTECTED_NAME", r.Errors?[0].Code);
    }

    // ── Table-level protection ────────────────────────────────

    [Fact]
    public void CreateTable_Protected_Error()
    {
        var r = _engine.ExecuteOne("create table _foo (name string 100)", "shop");

        Assert.Equal(SproutOperation.Error, r.Operation);
        Assert.Equal("PROTECTED_NAME", r.Errors?[0].Code);
        Assert.Contains("_foo", r.Errors?[0].Message ?? "");
    }

    [Fact]
    public void Upsert_ProtectedTable_Error()
    {
        var r = _engine.ExecuteOne("upsert _migrations {name: 'test', migrationorder: 1, executed: '2024-01-01 00:00:00'}", "shop");

        Assert.Equal(SproutOperation.Error, r.Operation);
        Assert.Equal("PROTECTED_NAME", r.Errors?[0].Code);
        Assert.Contains("_migrations", r.Errors?[0].Message ?? "");
    }

    [Fact]
    public void Delete_ProtectedTable_Error()
    {
        var r = _engine.ExecuteOne("delete _migrations where _id = 1", "shop");

        Assert.Equal(SproutOperation.Error, r.Operation);
        Assert.Equal("PROTECTED_NAME", r.Errors?[0].Code);
    }

    [Fact]
    public void PurgeTable_Protected_Error()
    {
        var r = _engine.ExecuteOne("purge table _migrations", "shop");

        Assert.Equal(SproutOperation.Error, r.Operation);
        Assert.Equal("PROTECTED_NAME", r.Errors?[0].Code);
    }

    // ── Column-level protection ───────────────────────────────

    [Fact]
    public void AddColumn_Protected_Error()
    {
        var r = _engine.ExecuteOne("add column users._bar string 100", "shop");

        Assert.Equal(SproutOperation.Error, r.Operation);
        Assert.Equal("PROTECTED_NAME", r.Errors?[0].Code);
        Assert.Contains("_bar", r.Errors?[0].Message ?? "");
    }

    [Fact]
    public void PurgeColumn_Protected_Error()
    {
        var r = _engine.ExecuteOne("purge column users._bar", "shop");

        Assert.Equal(SproutOperation.Error, r.Operation);
        Assert.Equal("PROTECTED_NAME", r.Errors?[0].Code);
    }

    [Fact]
    public void RenameColumn_ToProtected_Error()
    {
        var r = _engine.ExecuteOne("rename column users.name to _bar", "shop");

        Assert.Equal(SproutOperation.Error, r.Operation);
        Assert.Equal("PROTECTED_NAME", r.Errors?[0].Code);
    }

    [Fact]
    public void AlterColumn_Protected_Error()
    {
        var r = _engine.ExecuteOne("alter column users._bar string 200", "shop");

        Assert.Equal(SproutOperation.Error, r.Operation);
        Assert.Equal("PROTECTED_NAME", r.Errors?[0].Code);
    }

    [Fact]
    public void CreateIndex_ProtectedColumn_Error()
    {
        var r = _engine.ExecuteOne("create index users._bar", "shop");

        Assert.Equal(SproutOperation.Error, r.Operation);
        Assert.Equal("PROTECTED_NAME", r.Errors?[0].Code);
    }

    [Fact]
    public void PurgeIndex_ProtectedColumn_Error()
    {
        var r = _engine.ExecuteOne("purge index users._bar", "shop");

        Assert.Equal(SproutOperation.Error, r.Operation);
        Assert.Equal("PROTECTED_NAME", r.Errors?[0].Code);
    }

    // ── Reads are allowed ─────────────────────────────────────

    [Fact]
    public void Get_ProtectedTable_Allowed()
    {
        // Create _migrations through migration runner so it exists
        _engine.Migrate(typeof(WriteProtectionTests).Assembly, _engine.GetOrCreateDatabase("shop"));

        var r = _engine.ExecuteOne("get _migrations", "shop");

        // Should succeed (not PROTECTED_NAME error)
        Assert.NotEqual(SproutOperation.Error, r.Operation);
    }

    [Fact]
    public void Describe_ProtectedTable_Allowed()
    {
        _engine.Migrate(typeof(WriteProtectionTests).Assembly, _engine.GetOrCreateDatabase("shop"));

        var r = _engine.ExecuteOne("describe _migrations", "shop");

        Assert.NotEqual(SproutOperation.Error, r.Operation);
    }

    // ── _id in upsert is NOT a write ──────────────────────────

    [Fact]
    public void Upsert_WithIdField_Allowed()
    {
        var r = _engine.ExecuteOne("upsert users {_id: 1, name: 'Bob'}", "shop");

        Assert.Equal(SproutOperation.Upsert, r.Operation);
        Assert.Null(r.Errors);
    }

    // ── Migrations still work through internal bypass ─────────

    [Fact]
    public void Migrate_WorksWithProtection()
    {
        var db = _engine.GetOrCreateDatabase("testdb");
        _engine.Migrate(typeof(WriteProtectionTests).Assembly, db);

        // _migrations table should be queryable
        var r = _engine.ExecuteOne("get _migrations", "testdb");
        Assert.NotEqual(SproutOperation.Error, r.Operation);
    }
}

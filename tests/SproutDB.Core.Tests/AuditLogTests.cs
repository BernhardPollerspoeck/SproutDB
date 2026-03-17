namespace SproutDB.Core.Tests;

public class AuditLogTests : IDisposable
{
    private readonly string _tempDir;
    private readonly SproutEngine _engine;

    public AuditLogTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), $"sproutdb-test-{Guid.NewGuid()}");
        _engine = new SproutEngine(_tempDir);
        _engine.ExecuteOne("create database", "shop");
    }

    public void Dispose()
    {
        _engine.Dispose();
        if (Directory.Exists(_tempDir))
            Directory.Delete(_tempDir, true);
    }

    [Fact]
    public void CreateTable_LoggedInAuditLog()
    {
        _engine.ExecuteOne("create table users (name string 100)", "shop");

        var r = _engine.ExecuteOne("get audit_log where operation = 'create_table'", "_system");

        Assert.NotEqual(SproutOperation.Error, r.Operation);
        Assert.NotNull(r.Data);
        Assert.Single(r.Data);
        Assert.Equal("create_table", r.Data[0]["operation"]);
        Assert.Equal("shop", r.Data[0]["database"]);
        Assert.Contains("create table users", r.Data[0]["query"]?.ToString() ?? "");
    }

    [Fact]
    public void CreateDatabase_LoggedInAuditLog()
    {
        _engine.ExecuteOne("create database", "testdb");

        var r = _engine.ExecuteOne("get audit_log where operation = 'create_database'", "_system");

        Assert.NotEqual(SproutOperation.Error, r.Operation);
        Assert.NotNull(r.Data);
        // "shop" + "testdb" = 2 create_database entries
        Assert.Equal(2, r.Data.Count);
    }

    [Fact]
    public void MultipleSchemaChanges_AllLogged()
    {
        _engine.ExecuteOne("create table products (name string 100, price sint)", "shop");
        _engine.ExecuteOne("add column products.stock ubyte", "shop");
        _engine.ExecuteOne("rename column products.stock to inventory", "shop");
        _engine.ExecuteOne("create index products.name", "shop");

        var r = _engine.ExecuteOne("get audit_log", "_system");

        Assert.NotEqual(SproutOperation.Error, r.Operation);
        Assert.NotNull(r.Data);

        var operations = r.Data.Select(d => d["operation"]?.ToString()).ToList();
        // create_database (shop) + create_table + add_column + rename_column + create_index
        Assert.Contains("create_database", operations);
        Assert.Contains("create_table", operations);
        Assert.Contains("add_column", operations);
        Assert.Contains("rename_column", operations);
        Assert.Contains("create_index", operations);
    }

    [Fact]
    public void DataOperations_NotLogged()
    {
        _engine.ExecuteOne("create table users (name string 100)", "shop");
        _engine.ExecuteOne("upsert users {name: 'Alice'}", "shop");
        _engine.ExecuteOne("upsert users {name: 'Bob'}", "shop");
        _engine.ExecuteOne("delete users where name = 'Bob'", "shop");
        _engine.ExecuteOne("get users", "shop");

        var r = _engine.ExecuteOne("get audit_log", "_system");

        Assert.NotEqual(SproutOperation.Error, r.Operation);
        Assert.NotNull(r.Data);

        var operations = r.Data.Select(d => d["operation"]?.ToString()).ToList();
        Assert.DoesNotContain("upsert", operations);
        Assert.DoesNotContain("delete", operations);
        Assert.DoesNotContain("get", operations);
    }

    [Fact]
    public void AuditLog_Queryable_WithWhere()
    {
        _engine.ExecuteOne("create table t1 (a sint)", "shop");
        _engine.ExecuteOne("create table t2 (b sint)", "shop");

        var r = _engine.ExecuteOne("get audit_log where operation = 'create_table'", "_system");

        Assert.NotEqual(SproutOperation.Error, r.Operation);
        Assert.NotNull(r.Data);
        Assert.Equal(2, r.Data.Count);
    }

    [Fact]
    public void PurgeTable_LoggedInAuditLog()
    {
        _engine.ExecuteOne("create table temp (val sint)", "shop");
        _engine.ExecuteOne("purge table temp", "shop");

        var r = _engine.ExecuteOne("get audit_log where operation = 'purge_table'", "_system");

        Assert.NotEqual(SproutOperation.Error, r.Operation);
        Assert.NotNull(r.Data);
        Assert.Single(r.Data);
    }
}

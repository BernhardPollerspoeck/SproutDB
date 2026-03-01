namespace SproutDB.Core.Tests;

public class SystemDatabaseTests : IDisposable
{
    private readonly string _tempDir;
    private readonly SproutEngine _engine;

    public SystemDatabaseTests()
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

    [Fact]
    public void SystemDatabase_ExistsOnStartup()
    {
        var systemDbPath = Path.Combine(_tempDir, "_system");
        Assert.True(Directory.Exists(systemDbPath));
        Assert.True(File.Exists(Path.Combine(systemDbPath, "_meta.bin")));
    }

    [Fact]
    public void SystemDatabase_AuditLogTable_Exists()
    {
        var tablePath = Path.Combine(_tempDir, "_system", "audit_log");
        Assert.True(Directory.Exists(tablePath));
        Assert.True(File.Exists(Path.Combine(tablePath, "_schema.bin")));
        Assert.True(File.Exists(Path.Combine(tablePath, "_index")));
    }

    [Fact]
    public void SystemDatabase_IndexMetricsTable_Exists()
    {
        var tablePath = Path.Combine(_tempDir, "_system", "index_metrics");
        Assert.True(Directory.Exists(tablePath));
        Assert.True(File.Exists(Path.Combine(tablePath, "_schema.bin")));
        Assert.True(File.Exists(Path.Combine(tablePath, "_index")));
    }

    [Fact]
    public void SystemDatabase_AuditLog_Queryable()
    {
        var r = _engine.Execute("get audit_log", "_system");

        Assert.NotEqual(SproutOperation.Error, r.Operation);
    }

    [Fact]
    public void SystemDatabase_IndexMetrics_Queryable()
    {
        var r = _engine.Execute("get index_metrics", "_system");

        Assert.NotEqual(SproutOperation.Error, r.Operation);
    }

    [Fact]
    public void SystemDatabase_AuditLog_Describable()
    {
        var r = _engine.Execute("describe audit_log", "_system");

        Assert.Equal(SproutOperation.Describe, r.Operation);
        Assert.NotNull(r.Schema?.Columns);

        var colNames = r.Schema.Columns.Select(c => c.Name).ToList();
        Assert.Contains("timestamp", colNames);
        Assert.Contains("database", colNames);
        Assert.Contains("query", colNames);
        Assert.Contains("operation", colNames);
    }

    [Fact]
    public void SystemDatabase_IndexMetrics_Describable()
    {
        var r = _engine.Execute("describe index_metrics", "_system");

        Assert.Equal(SproutOperation.Describe, r.Operation);
        Assert.NotNull(r.Schema?.Columns);

        var colNames = r.Schema.Columns.Select(c => c.Name).ToList();
        Assert.Contains("key", colNames);
        Assert.Contains("table_name", colNames);
        Assert.Contains("column_name", colNames);
        Assert.Contains("query_count", colNames);
        Assert.Contains("is_manual", colNames);
    }

    [Fact]
    public void SystemDatabase_WriteProtected_Upsert()
    {
        var r = _engine.Execute("upsert audit_log {operation: 'test', query: 'test', timestamp: '2024-01-01 00:00:00'}", "_system");

        Assert.Equal(SproutOperation.Error, r.Operation);
        Assert.Equal("PROTECTED_NAME", r.Errors?[0].Code);
    }

    [Fact]
    public void SystemDatabase_WriteProtected_CreateTable()
    {
        var r = _engine.Execute("create table foo (name string 100)", "_system");

        Assert.Equal(SproutOperation.Error, r.Operation);
        Assert.Equal("PROTECTED_NAME", r.Errors?[0].Code);
    }

    [Fact]
    public void SystemDatabase_WriteProtected_Delete()
    {
        var r = _engine.Execute("delete audit_log where _id = 1", "_system");

        Assert.Equal(SproutOperation.Error, r.Operation);
        Assert.Equal("PROTECTED_NAME", r.Errors?[0].Code);
    }

    [Fact]
    public void SystemDatabase_SurvivesRestart()
    {
        // Use a separate temp dir so the fixture Dispose doesn't double-dispose
        var dir = Path.Combine(Path.GetTempPath(), $"sproutdb-test-{Guid.NewGuid()}");
        try
        {
            using (var engine1 = new SproutEngine(dir))
            {
                // _system should exist
                var r1 = engine1.Execute("get audit_log", "_system");
                Assert.NotEqual(SproutOperation.Error, r1.Operation);
            }

            // Re-open engine — should not crash, _system should still exist
            using var engine2 = new SproutEngine(dir);
            var r2 = engine2.Execute("get audit_log", "_system");
            Assert.NotEqual(SproutOperation.Error, r2.Operation);
        }
        finally
        {
            if (Directory.Exists(dir))
                Directory.Delete(dir, true);
        }
    }
}

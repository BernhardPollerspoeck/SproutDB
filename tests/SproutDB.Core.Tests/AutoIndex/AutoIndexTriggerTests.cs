using SproutDB.Core.AutoIndex;

namespace SproutDB.Core.Tests.AutoIndex;

public class AutoIndexTriggerTests : IDisposable
{
    private readonly string _tempDir;

    public AutoIndexTriggerTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), $"sproutdb-autoindex-{Guid.NewGuid()}");
    }

    public void Dispose()
    {
        if (Directory.Exists(_tempDir))
            Directory.Delete(_tempDir, true);
    }

    [Fact]
    public void AutoIndex_NotCreated_BelowMinimumQueryCount()
    {
        using var engine = new SproutEngine(new SproutEngineSettings
        {
            DataDirectory = _tempDir,
            FlushInterval = Timeout.InfiniteTimeSpan,
            AutoIndex = new AutoIndexSettings
            {
                Enabled = true,
                MinimumQueryCount = 10,
                UsageThreshold = 0.3,
                SelectivityThreshold = 0.0,
                ReadWriteRatio = 0.0,
            },
        });

        engine.Execute("create database", "shop");
        engine.Execute("create table users (name string 100, age ubyte)", "shop");

        // Only 5 queries — below MinimumQueryCount of 10
        for (int i = 0; i < 5; i++)
            engine.Execute("upsert users {name: 'User" + i + "', age: " + (20 + i) + "}", "shop");
        for (int i = 0; i < 5; i++)
            engine.Execute("get users where name = 'User" + i + "'", "shop");

        // Force flush (which triggers evaluation)
        engine.Dispose();

        // Verify no B-Tree was created
        var btreePath = Path.Combine(_tempDir, "shop", "users", "name.btree");
        Assert.False(File.Exists(btreePath));
    }

    [Fact]
    public void AutoIndex_Created_WhenThresholdsExceeded()
    {
        using var engine = new SproutEngine(new SproutEngineSettings
        {
            DataDirectory = _tempDir,
            FlushInterval = Timeout.InfiniteTimeSpan,
            AutoIndex = new AutoIndexSettings
            {
                Enabled = true,
                MinimumQueryCount = 5,
                UsageThreshold = 0.3,
                SelectivityThreshold = 0.0,
                ReadWriteRatio = 0.0,
            },
        });

        engine.Execute("create database", "shop");
        engine.Execute("create table users (name string 100, age ubyte)", "shop");

        // Insert some data
        for (int i = 0; i < 10; i++)
            engine.Execute("upsert users {name: 'User" + i + "', age: " + (20 + i) + "}", "shop");

        // Run enough WHERE queries to exceed thresholds
        for (int i = 0; i < 20; i++)
            engine.Execute("get users where name = 'User0'", "shop");

        // Force flush → triggers auto-index evaluation
        // Dispose calls FlushAll which calls EvaluateAutoIndexes
        engine.Dispose();

        // Verify B-Tree was created
        var btreePath = Path.Combine(_tempDir, "shop", "users", "name.btree");
        Assert.True(File.Exists(btreePath), "Expected auto-index B-Tree to be created");
    }

    [Fact]
    public void AutoIndex_NotCreated_WhenDisabled()
    {
        using var engine = new SproutEngine(new SproutEngineSettings
        {
            DataDirectory = _tempDir,
            FlushInterval = Timeout.InfiniteTimeSpan,
            AutoIndex = new AutoIndexSettings
            {
                Enabled = false,
                MinimumQueryCount = 5,
            },
        });

        engine.Execute("create database", "shop");
        engine.Execute("create table users (name string 100)", "shop");

        for (int i = 0; i < 10; i++)
            engine.Execute("upsert users {name: 'User" + i + "'}", "shop");
        for (int i = 0; i < 20; i++)
            engine.Execute("get users where name = 'User0'", "shop");

        engine.Dispose();

        var btreePath = Path.Combine(_tempDir, "shop", "users", "name.btree");
        Assert.False(File.Exists(btreePath));
    }

    [Fact]
    public void AutoIndex_ManualIndex_NotAutoRemoved()
    {
        using var engine = new SproutEngine(new SproutEngineSettings
        {
            DataDirectory = _tempDir,
            FlushInterval = Timeout.InfiniteTimeSpan,
            AutoIndex = new AutoIndexSettings
            {
                Enabled = true,
                MinimumQueryCount = 5,
                UnusedRetentionDays = 0, // Would remove auto-indexes immediately
            },
        });

        engine.Execute("create database", "shop");
        engine.Execute("create table users (name string 100)", "shop");
        engine.Execute("create index users.name", "shop");

        // No WHERE queries — index is "unused"
        engine.Dispose();

        // Manual index should still exist
        var btreePath = Path.Combine(_tempDir, "shop", "users", "name.btree");
        Assert.True(File.Exists(btreePath), "Manual index should not be auto-removed");
    }

    [Fact]
    public void AutoIndex_Created_LogsReason()
    {
        var dir = Path.Combine(Path.GetTempPath(), $"sproutdb-autoindex-log-{Guid.NewGuid()}");
        try
        {
            using (var engine = new SproutEngine(new SproutEngineSettings
            {
                DataDirectory = dir,
                FlushInterval = Timeout.InfiniteTimeSpan,
                AutoIndex = new AutoIndexSettings
                {
                    Enabled = true,
                    MinimumQueryCount = 5,
                    UsageThreshold = 0.3,
                    SelectivityThreshold = 0.0,
                    ReadWriteRatio = 0.0,
                },
            }))
            {
                engine.Execute("create database", "shop");
                engine.Execute("create table users (name string 100)", "shop");

                for (int i = 0; i < 10; i++)
                    engine.Execute("upsert users {name: 'User" + i + "'}", "shop");
                for (int i = 0; i < 20; i++)
                    engine.Execute("get users where name = 'User0'", "shop");
            }

            // Re-open and check audit log
            using var engine2 = new SproutEngine(dir);
            var r = engine2.Execute("get audit_log where operation = 'auto_create_index'", "_system");

            Assert.NotEqual(SproutOperation.Error, r.Operation);
            Assert.NotNull(r.Data);
            Assert.True(r.Data.Count > 0, "Expected audit log entry for auto-index creation");

            var entry = r.Data[0];
            var query = entry["query"]?.ToString();
            Assert.NotNull(query);
            Assert.Contains("auto-index created", query);
            Assert.Contains("where_hits=", query);
        }
        finally
        {
            if (Directory.Exists(dir))
                Directory.Delete(dir, true);
        }
    }

    [Fact]
    public void AutoIndex_NotCreated_OnColumnsNeverInWhere()
    {
        // Reproduces the sandbox scenario: create table, upsert data, get without WHERE
        // No auto-index should be created on ANY column
        using var engine = new SproutEngine(new SproutEngineSettings
        {
            DataDirectory = _tempDir,
            FlushInterval = Timeout.InfiniteTimeSpan,
            AutoIndex = new AutoIndexSettings
            {
                Enabled = true,
                MinimumQueryCount = 5,
                UsageThreshold = 0.3,
                SelectivityThreshold = 0.0,
                ReadWriteRatio = 0.0,
            },
        });

        engine.Execute("create database", "testdb");
        engine.Execute("create table users (name string 100, email string 320 strict, age ubyte, active bool default true, score sint)", "testdb");

        // Upserts create metrics entries for all columns via RecordWrite
        for (int i = 0; i < 10; i++)
            engine.Execute($"upsert users {{name: 'User{i}', email: 'user{i}@test.com', age: {20 + i}, score: {i * 10}}}", "testdb");

        // GETs without WHERE — should NOT trigger auto-indexing
        for (int i = 0; i < 20; i++)
            engine.Execute("get users", "testdb");

        // Flush triggers EvaluateAutoIndexes
        engine.Dispose();

        // No B-Tree should exist for any column
        var usersDir = Path.Combine(_tempDir, "testdb", "users");
        var btreeFiles = Directory.Exists(usersDir)
            ? Directory.GetFiles(usersDir, "*.btree")
            : [];
        Assert.Empty(btreeFiles);
    }

    [Fact]
    public void AutoIndex_OnlyCreated_ForWhereColumn_NotOthers()
    {
        using var engine = new SproutEngine(new SproutEngineSettings
        {
            DataDirectory = _tempDir,
            FlushInterval = Timeout.InfiniteTimeSpan,
            AutoIndex = new AutoIndexSettings
            {
                Enabled = true,
                MinimumQueryCount = 5,
                UsageThreshold = 0.3,
                SelectivityThreshold = 0.0,
                ReadWriteRatio = 0.0,
            },
        });

        engine.Execute("create database", "testdb");
        engine.Execute("create table users (name string 100, email string 320, age ubyte, score sint)", "testdb");

        for (int i = 0; i < 10; i++)
            engine.Execute($"upsert users {{name: 'User{i}', email: 'u{i}@test.com', age: {20 + i}, score: {i * 10}}}", "testdb");

        // Only query WHERE on 'name' — other columns should NOT be auto-indexed
        for (int i = 0; i < 20; i++)
            engine.Execute("get users where name = 'User0'", "testdb");

        engine.Dispose();

        var usersDir = Path.Combine(_tempDir, "testdb", "users");
        Assert.True(File.Exists(Path.Combine(usersDir, "name.btree")), "name should be auto-indexed");
        Assert.False(File.Exists(Path.Combine(usersDir, "email.btree")), "email should NOT be auto-indexed");
        Assert.False(File.Exists(Path.Combine(usersDir, "age.btree")), "age should NOT be auto-indexed");
        Assert.False(File.Exists(Path.Combine(usersDir, "score.btree")), "score should NOT be auto-indexed");
    }

    [Fact]
    public void AutoIndex_IsNotMarkedAsManual()
    {
        var dir = Path.Combine(Path.GetTempPath(), $"sproutdb-autoindex-manual-{Guid.NewGuid()}");
        try
        {
            using (var engine = new SproutEngine(new SproutEngineSettings
            {
                DataDirectory = dir,
                FlushInterval = Timeout.InfiniteTimeSpan,
                AutoIndex = new AutoIndexSettings
                {
                    Enabled = true,
                    MinimumQueryCount = 5,
                    UsageThreshold = 0.3,
                    SelectivityThreshold = 0.0,
                    ReadWriteRatio = 0.0,
                },
            }))
            {
                engine.Execute("create database", "shop");
                engine.Execute("create table users (name string 100)", "shop");

                for (int i = 0; i < 10; i++)
                    engine.Execute($"upsert users {{name: 'User{i}'}}", "shop");
                for (int i = 0; i < 20; i++)
                    engine.Execute("get users where name = 'User0'", "shop");
            }

            // Re-open and check index_metrics
            using var engine2 = new SproutEngine(dir);
            var r = engine2.Execute("get index_metrics", "_system");

            Assert.NotNull(r.Data);
            var nameEntry = r.Data.FirstOrDefault(row =>
                row.TryGetValue("column_name", out var col) && col?.ToString() == "name"
                && row.TryGetValue("table_name", out var tbl) && tbl?.ToString() == "users");

            Assert.NotNull(nameEntry);
            nameEntry.TryGetValue("is_manual", out var isManual);
            Assert.False(isManual is true, "Auto-created index should NOT be marked as manual");
            nameEntry.TryGetValue("index_created_at", out var createdAt);
            Assert.NotNull(createdAt);
        }
        finally
        {
            if (Directory.Exists(dir))
                Directory.Delete(dir, true);
        }
    }

    // ── AutoIndexEvaluator unit tests ────────────────────────

    [Fact]
    public void ShouldCreate_AllConditionsMet_ReturnsTrue()
    {
        var metrics = new IndexMetrics();
        metrics.Load(
            queryCount: 100, whereHitCount: 50, readCount: 100, writeCount: 10,
            scannedTotal: 1000, resultTotal: 10, isManual: false,
            lastUsedAt: DateTime.UtcNow, indexCreatedAt: null);

        var settings = new AutoIndexSettings
        {
            Enabled = true,
            MinimumQueryCount = 10,
            UsageThreshold = 0.3,
            SelectivityThreshold = 0.5,
            ReadWriteRatio = 3.0,
        };

        Assert.True(AutoIndexEvaluator.ShouldCreate(metrics, settings));
    }

    [Fact]
    public void ShouldCreate_BelowUsageThreshold_ReturnsFalse()
    {
        var metrics = new IndexMetrics();
        metrics.Load(
            queryCount: 100, whereHitCount: 10, readCount: 100, writeCount: 10,
            scannedTotal: 1000, resultTotal: 10, isManual: false,
            lastUsedAt: DateTime.UtcNow, indexCreatedAt: null);

        var settings = new AutoIndexSettings
        {
            Enabled = true,
            UsageThreshold = 0.3,
        };

        Assert.False(AutoIndexEvaluator.ShouldCreate(metrics, settings));
    }

    [Fact]
    public void ShouldRemove_Manual_ReturnsFalse()
    {
        var metrics = new IndexMetrics();
        metrics.Load(
            queryCount: 0, whereHitCount: 0, readCount: 0, writeCount: 0,
            scannedTotal: 0, resultTotal: 0, isManual: true,
            lastUsedAt: DateTime.UtcNow.AddDays(-60), indexCreatedAt: DateTime.UtcNow.AddDays(-90));

        var settings = new AutoIndexSettings { UnusedRetentionDays = 30 };

        Assert.False(AutoIndexEvaluator.ShouldRemove(metrics, settings, DateTime.UtcNow));
    }
}

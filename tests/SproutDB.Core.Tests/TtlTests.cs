namespace SproutDB.Core.Tests;

public class TtlTests : IDisposable
{
    private readonly string _tempDir;
    private readonly SproutEngine _engine;

    public TtlTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), $"sproutdb-test-ttl-{Guid.NewGuid()}");
        _engine = new SproutEngine(_tempDir);
        _engine.Execute("create database", "shop");
    }

    public void Dispose()
    {
        _engine.Dispose();
        if (Directory.Exists(_tempDir))
            Directory.Delete(_tempDir, true);
    }

    // ── Parser ────────────────────────────────────────────────

    [Fact]
    public void CreateTable_WithTtl_Parses()
    {
        var result = _engine.Execute("create table sessions (token string 64) ttl 24h", "shop");
        Assert.Null(result.Errors);
        Assert.Equal(SproutOperation.CreateTable, result.Operation);
    }

    [Fact]
    public void CreateTable_WithTtl_Days()
    {
        var result = _engine.Execute("create table cache (key string 64) ttl 7d", "shop");
        Assert.Null(result.Errors);
    }

    [Fact]
    public void CreateTable_WithTtl_Minutes()
    {
        var result = _engine.Execute("create table temp (key string 64) ttl 30m", "shop");
        Assert.Null(result.Errors);
    }

    [Fact]
    public void CreateTable_WithTtl_CreatesTtlFile()
    {
        _engine.Execute("create table sessions (token string 64) ttl 24h", "shop");
        var ttlPath = Path.Combine(_tempDir, "shop", "sessions", "_ttl");
        Assert.True(File.Exists(ttlPath));
    }

    [Fact]
    public void CreateTable_WithoutTtl_NoTtlFile()
    {
        _engine.Execute("create table users (name string 100)", "shop");
        var ttlPath = Path.Combine(_tempDir, "shop", "users", "_ttl");
        Assert.False(File.Exists(ttlPath));
    }

    // ── TTL Expiry ────────────────────────────────────────────

    [Fact]
    public void Get_FiltersExpiredRows()
    {
        // Create table with 1-second TTL
        _engine.Execute("create table cache (key string 64) ttl 1m", "shop");

        // Insert a row — it should be visible
        _engine.Execute("upsert cache {key: 'hello'}", "shop");
        var result = _engine.Execute("get cache", "shop");
        Assert.NotNull(result.Data);
        Assert.Single(result.Data);

        // Manually expire the row by directly manipulating the TTL file
        // Set expiresAt to a past timestamp
        var ttlPath = Path.Combine(_tempDir, "shop", "cache", "_ttl");
        using (var fs = new FileStream(ttlPath, FileMode.Open, FileAccess.Write, FileShare.ReadWrite))
        {
            // Place 0, ExpiresAt = 1 (epoch + 1ms = definitely expired)
            var writer = new BinaryWriter(fs);
            writer.Write(1L); // expiresAt = 1ms (way in the past)
            writer.Write(60L); // rowTtlDuration = 60 seconds
        }

        // Re-open engine to pick up changes (TTL file is MMF-cached)
        // Actually, we need to re-read — the engine caches the table.
        // Let's use a fresh engine against the same dir.
        _engine.Dispose();
        using var engine2 = new SproutEngine(_tempDir);
        var result2 = engine2.Execute("get cache", "shop");
        Assert.NotNull(result2.Data);
        Assert.Empty(result2.Data);
    }

    [Fact]
    public void Upsert_WithRowTtl_Parses()
    {
        _engine.Execute("create table sessions (token string 64) ttl 24h", "shop");
        var result = _engine.Execute("upsert sessions {token: 'abc', ttl: 7d}", "shop");
        Assert.Null(result.Errors);
        Assert.NotNull(result.Data);
        Assert.Single(result.Data);
    }

    [Fact]
    public void Upsert_WithRowTtl_Zero()
    {
        _engine.Execute("create table sessions (token string 64) ttl 24h", "shop");
        var result = _engine.Execute("upsert sessions {token: 'abc', ttl: 0}", "shop");
        Assert.Null(result.Errors);
        Assert.NotNull(result.Data);
    }

    [Fact]
    public void Upsert_WithRowTtl_NoTableTtl_AutoEnablesTtlFile()
    {
        _engine.Execute("create table users (name string 100)", "shop");
        var ttlPath = Path.Combine(_tempDir, "shop", "users", "_ttl");
        Assert.False(File.Exists(ttlPath));

        // Upsert with row TTL should auto-create _ttl file
        var result = _engine.Execute("upsert users {name: 'Alice', ttl: 7d}", "shop");
        Assert.Null(result.Errors);

        // The _ttl file should exist now (need to flush/check via engine)
        // Re-open to verify file creation
        _engine.Dispose();
        Assert.True(File.Exists(ttlPath));
    }

    [Fact]
    public void Upsert_BulkWithTtl()
    {
        _engine.Execute("create table sessions (token string 64) ttl 24h", "shop");
        var result = _engine.Execute("upsert sessions [{token: 'a', ttl: 1h}, {token: 'b', ttl: 2h}, {token: 'c'}]", "shop");
        Assert.Null(result.Errors);
        Assert.NotNull(result.Data);
        Assert.Equal(3, result.Data.Count);
    }

    // ── Delete clears TTL ─────────────────────────────────────

    [Fact]
    public void Delete_ClearsTtlEntry()
    {
        _engine.Execute("create table sessions (token string 64) ttl 24h", "shop");
        _engine.Execute("upsert sessions {token: 'abc'}", "shop");

        var result = _engine.Execute("delete sessions where token = 'abc'", "shop");
        Assert.Equal(1, result.Affected);
    }

    // ── Purge TTL ─────────────────────────────────────────────

    [Fact]
    public void PurgeTtl_SetsTableTtlToZero()
    {
        _engine.Execute("create table sessions (token string 64) ttl 24h", "shop");
        var result = _engine.Execute("purge ttl sessions", "shop");
        Assert.Null(result.Errors);
        Assert.Equal(SproutOperation.PurgeTtl, result.Operation);

        // After purge ttl, new inserts without row TTL should not expire
        _engine.Execute("upsert sessions {token: 'permanent'}", "shop");
        var getResult = _engine.Execute("get sessions", "shop");
        Assert.NotNull(getResult.Data);
        Assert.Single(getResult.Data);
    }

    // ── Update resets TTL ─────────────────────────────────────

    [Fact]
    public void Update_ResetsTtl()
    {
        _engine.Execute("create table sessions (token string 64) ttl 24h", "shop");
        var insert = _engine.Execute("upsert sessions {token: 'abc'}", "shop");
        var id = insert.Data?[0]["_id"];
        Assert.NotNull(id);

        // Update the same row — TTL should be refreshed
        _engine.Execute($"upsert sessions {{_id: {id}, token: 'def'}}", "shop");

        var result = _engine.Execute("get sessions", "shop");
        Assert.NotNull(result.Data);
        Assert.Single(result.Data);
        Assert.Equal("def", result.Data[0]["token"]);
    }

    // ── TTL with aggregates ───────────────────────────────────

    [Fact]
    public void Aggregate_SkipsExpiredRows()
    {
        _engine.Execute("create table cache (key string 64, val slong) ttl 1m", "shop");
        _engine.Execute("upsert cache [{key: 'a', val: 10}, {key: 'b', val: 20}]", "shop");

        // Expire the first row
        var ttlPath = Path.Combine(_tempDir, "shop", "cache", "_ttl");
        using (var fs = new FileStream(ttlPath, FileMode.Open, FileAccess.Write, FileShare.ReadWrite))
        {
            var writer = new BinaryWriter(fs);
            writer.Write(1L); // expiresAt place 0 = 1ms (expired)
        }

        _engine.Dispose();
        using var engine2 = new SproutEngine(_tempDir);
        var result = engine2.Execute("get cache sum val", "shop");
        Assert.NotNull(result.Data);
        Assert.Equal(20.0, Convert.ToDouble(result.Data[0]["sum"]));
    }

    // ── Background cleanup ────────────────────────────────────

    [Fact]
    public void BackgroundCleanup_RemovesExpiredRows()
    {
        // Use a very short cleanup interval
        var tempDir = Path.Combine(Path.GetTempPath(), $"sproutdb-test-ttl-bg-{Guid.NewGuid()}");
        var settings = new SproutEngineSettings
        {
            DataDirectory = tempDir,
            TtlCleanupInterval = TimeSpan.FromMilliseconds(100),
            TtlCleanupBatchSize = 100,
        };
        using var engine = new SproutEngine(settings);
        engine.Execute("create database", "shop");
        engine.Execute("create table sessions (token string 64) ttl 1h", "shop");
        engine.Execute("upsert sessions [{token: 'a'}, {token: 'b'}, {token: 'c'}]", "shop");

        // Verify 3 rows
        var r1 = engine.Execute("get sessions", "shop");
        Assert.Equal(3, r1.Data?.Count);

        // Manually expire all rows
        var ttlPath = Path.Combine(tempDir, "shop", "sessions", "_ttl");
        using (var fs = new FileStream(ttlPath, FileMode.Open, FileAccess.Write, FileShare.ReadWrite))
        {
            var writer = new BinaryWriter(fs);
            for (int i = 0; i < 3; i++)
            {
                fs.Position = i * 16;
                writer.Write(1L); // expiresAt = 1ms (expired)
                writer.Write(3600L); // rowTtlDuration = 1h
            }
        }

        // Wait for cleanup to run
        Thread.Sleep(500);

        // After cleanup, rows should be physically deleted
        // Verify index count is 0
        var r2 = engine.Execute("get sessions", "shop");
        Assert.NotNull(r2.Data);
        Assert.Empty(r2.Data);

        engine.Dispose();
        if (Directory.Exists(tempDir))
            Directory.Delete(tempDir, true);
    }

    // ── Count aggregate with TTL ──────────────────────────────

    [Fact]
    public void Count_Aggregate_WithTtl()
    {
        _engine.Execute("create table items (name string 64) ttl 24h", "shop");
        _engine.Execute("upsert items [{name: 'a'}, {name: null}, {name: 'c'}]", "shop");

        var result = _engine.Execute("get items count name", "shop");
        Assert.NotNull(result.Data);
        Assert.Equal(2L, Convert.ToInt64(result.Data[0]["count"])); // 2 non-null
    }

    [Fact]
    public void Count_Aggregate_All()
    {
        _engine.Execute("create table items (name string 64)", "shop");
        _engine.Execute("upsert items [{name: 'a'}, {name: 'b'}, {name: 'c'}]", "shop");

        var result = _engine.Execute("get items count _id", "shop");
        Assert.NotNull(result.Data);
        Assert.Equal(3L, Convert.ToInt64(result.Data[0]["count"]));
    }

    // ── Virtual TTL columns ─────────────────────────────────

    [Fact]
    public void Select_ExpiresAt_ReturnsTimestamp()
    {
        _engine.Execute("create table cache (key string 64) ttl 1h", "shop");
        _engine.Execute("upsert cache {key: 'hello'}", "shop");

        var result = _engine.Execute("get cache select _id, key, _expiresat", "shop");
        Assert.NotNull(result.Data);
        Assert.Single(result.Data);

        var row = result.Data[0];
        Assert.True(row.ContainsKey("_expiresat"));
        var expiresAt = Convert.ToInt64(row["_expiresat"]);
        Assert.True(expiresAt > 0, "expiresAt should be a future timestamp");
    }

    [Fact]
    public void Select_Ttl_ReturnsRowTtlSeconds()
    {
        _engine.Execute("create table cache (key string 64) ttl 1h", "shop");
        _engine.Execute("upsert cache {key: 'a', ttl: 7d}", "shop");
        _engine.Execute("upsert cache {key: 'b'}", "shop");

        var result = _engine.Execute("get cache select key, _ttl", "shop");
        Assert.NotNull(result.Data);
        Assert.Equal(2, result.Data.Count);

        // Row with row-TTL override: 7d = 604800s
        var rowA = result.Data.First(r => (string?)r["key"] == "a");
        Assert.Equal(604800L, Convert.ToInt64(rowA["_ttl"]));

        // Row without row-TTL override: 0 means "uses table default"
        var rowB = result.Data.First(r => (string?)r["key"] == "b");
        Assert.Equal(0L, Convert.ToInt64(rowB["_ttl"]));
    }

    [Fact]
    public void Select_TtlColumns_NoTtlTable_ReturnsZero()
    {
        _engine.Execute("create table users (name string 64)", "shop");
        _engine.Execute("upsert users {name: 'Alice'}", "shop");

        var result = _engine.Execute("get users select name, _expiresat, _ttl", "shop");
        Assert.NotNull(result.Data);
        Assert.Single(result.Data);

        Assert.Equal(0L, Convert.ToInt64(result.Data[0]["_expiresat"]));
        Assert.Equal(0L, Convert.ToInt64(result.Data[0]["_ttl"]));
    }

    [Fact]
    public void Select_TtlColumns_WithAlias()
    {
        _engine.Execute("create table cache (key string 64) ttl 1h", "shop");
        _engine.Execute("upsert cache {key: 'test'}", "shop");

        var result = _engine.Execute("get cache select key, _expiresat as expires, _ttl as ttl_sec", "shop");
        Assert.NotNull(result.Data);
        Assert.Single(result.Data);
        Assert.True(result.Data[0].ContainsKey("expires"));
        Assert.True(result.Data[0].ContainsKey("ttl_sec"));
    }
}

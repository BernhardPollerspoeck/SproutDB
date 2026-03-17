namespace SproutDB.Core.Tests;

public class ChunkSizeTests : IDisposable
{
    private readonly string _tempDir;
    private readonly SproutEngine _engine;

    public ChunkSizeTests()
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

    // ── create database with chunk_size ──────────────────────

    [Fact]
    public void CreateDatabase_WithChunkSize_Success()
    {
        var r = _engine.ExecuteOne("create database with chunk_size 500", "admin");

        Assert.Equal(SproutOperation.CreateDatabase, r.Operation);
        Assert.Null(r.Errors);
    }

    [Fact]
    public void CreateDatabase_WithChunkSize_StoresInMeta()
    {
        _engine.ExecuteOne("create database with chunk_size 500", "admin");

        var metaPath = Path.Combine(_tempDir, "admin", "_meta.bin");
        using var fs = new FileStream(metaPath, FileMode.Open, FileAccess.Read);
        using var br = new BinaryReader(fs);
        var ticks = br.ReadInt64();
        var chunkSize = br.ReadInt32();

        Assert.True(ticks > 0);
        Assert.Equal(500, chunkSize);
    }

    [Fact]
    public void CreateDatabase_WithoutChunkSize_ChunkSizeIsZero()
    {
        _engine.ExecuteOne("create database", "admin");

        var metaPath = Path.Combine(_tempDir, "admin", "_meta.bin");
        using var fs = new FileStream(metaPath, FileMode.Open, FileAccess.Read);
        using var br = new BinaryReader(fs);
        br.ReadInt64(); // ticks
        var chunkSize = br.ReadInt32();

        Assert.Equal(0, chunkSize);
    }

    [Fact]
    public void CreateDatabase_ChunkSizeTooSmall_Error()
    {
        var r = _engine.ExecuteOne("create database with chunk_size 50", "admin");

        Assert.Equal(SproutOperation.Error, r.Operation);
        Assert.NotNull(r.Errors);
        Assert.Contains("chunk_size must be between 100 and 1000000", r.Errors[0].Message);
    }

    [Fact]
    public void CreateDatabase_ChunkSizeTooLarge_Error()
    {
        var r = _engine.ExecuteOne("create database with chunk_size 2000000", "admin");

        Assert.Equal(SproutOperation.Error, r.Operation);
        Assert.NotNull(r.Errors);
    }

    [Fact]
    public void CreateDatabase_ChunkSizeAtMinBoundary_Success()
    {
        var r = _engine.ExecuteOne("create database with chunk_size 100", "admin");

        Assert.Equal(SproutOperation.CreateDatabase, r.Operation);
        Assert.Null(r.Errors);
    }

    [Fact]
    public void CreateDatabase_ChunkSizeAtMaxBoundary_Success()
    {
        var r = _engine.ExecuteOne("create database with chunk_size 1000000", "admin");

        Assert.Equal(SproutOperation.CreateDatabase, r.Operation);
        Assert.Null(r.Errors);
    }

    // ── create table with chunk_size ─────────────────────────

    [Fact]
    public void CreateTable_WithChunkSize_Success()
    {
        _engine.ExecuteOne("create database", "testdb");
        var r = _engine.ExecuteOne("create table users (name string) with chunk_size 200", "testdb");

        Assert.Equal(SproutOperation.CreateTable, r.Operation);
        Assert.Null(r.Errors);
    }

    [Fact]
    public void CreateTable_WithChunkSize_StoresInSchema()
    {
        _engine.ExecuteOne("create database", "testdb");
        _engine.ExecuteOne("create table users (name string) with chunk_size 200", "testdb");

        var r = _engine.ExecuteOne("describe users", "testdb");
        Assert.Equal(200, r.Schema?.ChunkSize);
    }

    [Fact]
    public void CreateTable_WithTtlAndChunkSize_Success()
    {
        _engine.ExecuteOne("create database", "testdb");
        var r = _engine.ExecuteOne("create table logs (msg string 500) ttl 24h with chunk_size 300", "testdb");

        Assert.Equal(SproutOperation.CreateTable, r.Operation);
        Assert.Null(r.Errors);

        var desc = _engine.ExecuteOne("describe logs", "testdb");
        Assert.Equal(300, desc.Schema?.ChunkSize);
        Assert.Equal(86400, desc.Schema?.TtlSeconds);
    }

    [Fact]
    public void CreateTable_ChunkSizeTooSmall_Error()
    {
        _engine.ExecuteOne("create database", "testdb");
        var r = _engine.ExecuteOne("create table users (name string) with chunk_size 10", "testdb");

        Assert.Equal(SproutOperation.Error, r.Operation);
        Assert.NotNull(r.Errors);
    }

    [Fact]
    public void CreateTable_ChunkSizeUsesSmallFileSize()
    {
        _engine.ExecuteOne("create database", "testdb");
        _engine.ExecuteOne("create table users (name string 100) with chunk_size 200", "testdb");

        // Index file should be HEADER(20) + 200 * 8 = 1620 bytes
        var indexPath = Path.Combine(_tempDir, "testdb", "users", "_index");
        Assert.Equal(20 + 200 * 8, new FileInfo(indexPath).Length);

        // Column file should be 200 * entrySize(101) = 20200 bytes
        var colPath = Path.Combine(_tempDir, "testdb", "users", "name.col");
        Assert.Equal(200 * 101, new FileInfo(colPath).Length);
    }

    // ── ChunkSize Kaskade ────────────────────────────────────

    [Fact]
    public void CreateTable_InheritsDbChunkSize()
    {
        _engine.ExecuteOne("create database with chunk_size 300", "testdb");
        _engine.ExecuteOne("create table users (name string 100)", "testdb");

        // Index file should use DB chunk_size (300 slots)
        var indexPath = Path.Combine(_tempDir, "testdb", "users", "_index");
        Assert.Equal(20 + 300 * 8, new FileInfo(indexPath).Length);
    }

    [Fact]
    public void CreateTable_TableChunkSizeOverridesDb()
    {
        _engine.ExecuteOne("create database with chunk_size 300", "testdb");
        _engine.ExecuteOne("create table users (name string 100) with chunk_size 150", "testdb");

        // Table-level should win: 150 slots
        var indexPath = Path.Combine(_tempDir, "testdb", "users", "_index");
        Assert.Equal(20 + 150 * 8, new FileInfo(indexPath).Length);
    }

    [Fact]
    public void CreateTable_EngineDefaultWhenNoDbOrTableChunkSize()
    {
        _engine.ExecuteOne("create database", "testdb");
        _engine.ExecuteOne("create table users (name string 100)", "testdb");

        // Engine default is 10000 slots
        var indexPath = Path.Combine(_tempDir, "testdb", "users", "_index");
        Assert.Equal(20 + 10_000 * 8, new FileInfo(indexPath).Length);
    }

    // ── describe shows chunk_size ────────────────────────────

    [Fact]
    public void DescribeTable_ShowsChunkSize()
    {
        _engine.ExecuteOne("create database", "testdb");
        _engine.ExecuteOne("create table users (name string) with chunk_size 500", "testdb");

        var r = _engine.ExecuteOne("describe users", "testdb");
        Assert.Equal(500, r.Schema?.ChunkSize);
        Assert.Equal(500, r.Schema?.EffectiveChunkSize);
    }

    [Fact]
    public void DescribeTable_EffectiveFromDb()
    {
        _engine.ExecuteOne("create database with chunk_size 400", "testdb");
        _engine.ExecuteOne("create table users (name string)", "testdb");

        var r = _engine.ExecuteOne("describe users", "testdb");
        Assert.Equal(0, r.Schema?.ChunkSize); // no table-level
        Assert.Equal(400, r.Schema?.EffectiveChunkSize);
    }

    [Fact]
    public void DescribeAll_ShowsDbChunkSize()
    {
        _engine.ExecuteOne("create database with chunk_size 400", "testdb");

        var r = _engine.ExecuteOne("describe", "testdb");
        Assert.Equal(400, r.Schema?.ChunkSize);
    }

    [Fact]
    public void DescribeAll_NoChunkSize_ReturnsZero()
    {
        _engine.ExecuteOne("create database", "testdb");

        var r = _engine.ExecuteOne("describe", "testdb");
        Assert.Equal(0, r.Schema?.ChunkSize);
    }

    // ── backward compatibility ───────────────────────────────

    [Fact]
    public void OldMetaFile_WithoutChunkSize_ReadsAsZero()
    {
        // Write old-format meta (8 bytes only)
        var dbPath = Path.Combine(_tempDir, "olddb");
        Directory.CreateDirectory(dbPath);
        var metaPath = Path.Combine(dbPath, "_meta.bin");
        using (var fs = new FileStream(metaPath, FileMode.Create, FileAccess.Write))
        using (var bw = new BinaryWriter(fs))
        {
            bw.Write(DateTime.UtcNow.Ticks); // only 8 bytes, no chunk_size
        }

        Assert.Equal(8, new FileInfo(metaPath).Length);

        using var fs2 = new FileStream(metaPath, FileMode.Open, FileAccess.Read);
        using var br = new BinaryReader(fs2);
        var ticks = br.ReadInt64();
        Assert.True(ticks > 0);
    }

    // ── shrink table ─────────────────────────────────────────

    [Fact]
    public void ShrinkTable_ReducesFileSize()
    {
        _engine.ExecuteOne("create database", "testdb");
        _engine.ExecuteOne("create table users (name string 100)", "testdb");

        for (int i = 0; i < 5; i++)
            _engine.ExecuteOne($"upsert users {{name: 'user{i}'}}", "testdb");

        var beforeIndex = new FileInfo(Path.Combine(_tempDir, "testdb", "users", "_index")).Length;

        var r = _engine.ExecuteOne("shrink table users chunk_size 100", "testdb");
        Assert.Equal(SproutOperation.ShrinkTable, r.Operation);
        Assert.Null(r.Errors);

        var afterIndex = new FileInfo(Path.Combine(_tempDir, "testdb", "users", "_index")).Length;
        Assert.True(afterIndex < beforeIndex, $"Expected {afterIndex} < {beforeIndex}");

        // Should be 100 slots: HEADER(20) + 100 * 8 = 820
        Assert.Equal(20 + 100 * 8, afterIndex);
    }

    [Fact]
    public void ShrinkTable_PreservesAllRows()
    {
        _engine.ExecuteOne("create database", "testdb");
        _engine.ExecuteOne("create table users (name string 100, age ubyte)", "testdb");

        for (int i = 0; i < 10; i++)
            _engine.ExecuteOne($"upsert users {{name: 'user{i}', age: {i + 20}}}", "testdb");

        // Verify rows exist before shrink
        var before = _engine.ExecuteOne("get users", "testdb");
        Assert.Equal(10, before.Data?.Count);

        var r = _engine.ExecuteOne("shrink table users chunk_size 100", "testdb");
        Assert.Equal(SproutOperation.ShrinkTable, r.Operation);
        Assert.Null(r.Errors);

        var after = _engine.ExecuteOne("get users", "testdb");
        Assert.NotNull(after.Data);
        Assert.Equal(10, after.Data.Count);

        // Check data integrity — all names present
        var names = after.Data.Select(row => (string)row["name"]).OrderBy(n => n).ToList();
        for (int i = 0; i < 10; i++)
            Assert.Contains($"user{i}", names);
    }

    [Fact]
    public void ShrinkTable_UpdatesSchemaChunkSize()
    {
        _engine.ExecuteOne("create database", "testdb");
        _engine.ExecuteOne("create table users (name string 100)", "testdb");

        _engine.ExecuteOne("shrink table users chunk_size 200", "testdb");

        var desc = _engine.ExecuteOne("describe users", "testdb");
        Assert.Equal(200, desc.Schema?.ChunkSize);
    }

    [Fact]
    public void ShrinkTable_ClosesGaps()
    {
        _engine.ExecuteOne("create database", "testdb");
        _engine.ExecuteOne("create table users (name string 100)", "testdb");

        for (int i = 0; i < 5; i++)
            _engine.ExecuteOne($"upsert users {{name: 'user{i}'}}", "testdb");

        // Delete rows 2 and 4 to create gaps
        _engine.ExecuteOne("delete users where _id = 2", "testdb");
        _engine.ExecuteOne("delete users where _id = 4", "testdb");

        _engine.ExecuteOne("shrink table users chunk_size 100", "testdb");

        var r = _engine.ExecuteOne("get users", "testdb");
        Assert.NotNull(r.Data);
        Assert.Equal(3, r.Data.Count);
    }

    [Fact]
    public void ShrinkTable_WithTtl_PreservesTtlData()
    {
        _engine.ExecuteOne("create database", "testdb");
        _engine.ExecuteOne("create table events (name string 100) ttl 24h", "testdb");

        for (int i = 0; i < 3; i++)
            _engine.ExecuteOne($"upsert events {{name: 'ev{i}'}}", "testdb");

        _engine.ExecuteOne("shrink table events chunk_size 100", "testdb");

        // TTL file should still exist
        Assert.True(File.Exists(Path.Combine(_tempDir, "testdb", "events", "_ttl")));

        var r = _engine.ExecuteOne("get events", "testdb");
        Assert.Equal(3, r.Data?.Count);
    }

    [Fact]
    public void ShrinkTable_WithoutChunkSize_UsesExisting()
    {
        _engine.ExecuteOne("create database", "testdb");
        _engine.ExecuteOne("create table users (name string) with chunk_size 200", "testdb");

        for (int i = 0; i < 3; i++)
            _engine.ExecuteOne($"upsert users {{name: 'user{i}'}}", "testdb");

        _engine.ExecuteOne("shrink table users", "testdb");

        // Should have used existing chunk_size 200
        var indexSize = new FileInfo(Path.Combine(_tempDir, "testdb", "users", "_index")).Length;
        Assert.Equal(20 + 200 * 8, indexSize);
    }

    [Fact]
    public void ShrinkTable_ReturnsBeforeAfterStats()
    {
        _engine.ExecuteOne("create database", "testdb");
        _engine.ExecuteOne("create table users (name string 100)", "testdb");

        for (int i = 0; i < 5; i++)
            _engine.ExecuteOne($"upsert users {{name: 'user{i}'}}", "testdb");

        var r = _engine.ExecuteOne("shrink table users chunk_size 100", "testdb");

        Assert.NotNull(r.Data);
        Assert.Single(r.Data);
        var row = r.Data[0];
        Assert.True((long)row["before_bytes"] > (long)row["after_bytes"]);
        Assert.Equal(5, (int)row["rows"]);
        Assert.Equal(100, (int)row["chunk_size"]);
    }

    [Fact]
    public void ShrinkTable_EmptyTable_SetsToChunkSize()
    {
        _engine.ExecuteOne("create database", "testdb");
        _engine.ExecuteOne("create table users (name string 100)", "testdb");

        _engine.ExecuteOne("shrink table users chunk_size 100", "testdb");

        var indexSize = new FileInfo(Path.Combine(_tempDir, "testdb", "users", "_index")).Length;
        Assert.Equal(20 + 100 * 8, indexSize);
    }

    [Fact]
    public void ShrinkTable_ManyRowsNeedMultipleChunks()
    {
        _engine.ExecuteOne("create database", "testdb");
        _engine.ExecuteOne("create table users (name string 30)", "testdb");

        // Insert 250 rows via bulk
        var bulk = string.Join(", ", Enumerable.Range(0, 100).Select(i => $"{{name: 'u{i}'}}"));
        _engine.ExecuteOne($"upsert users [{bulk}]", "testdb");
        bulk = string.Join(", ", Enumerable.Range(100, 100).Select(i => $"{{name: 'u{i}'}}"));
        _engine.ExecuteOne($"upsert users [{bulk}]", "testdb");
        bulk = string.Join(", ", Enumerable.Range(200, 50).Select(i => $"{{name: 'u{i}'}}"));
        _engine.ExecuteOne($"upsert users [{bulk}]", "testdb");

        _engine.ExecuteOne("shrink table users chunk_size 100", "testdb");

        // ceil(250 / 100) * 100 = 300 slots
        var indexSize = new FileInfo(Path.Combine(_tempDir, "testdb", "users", "_index")).Length;
        Assert.Equal(20 + 300 * 8, indexSize);
    }

    [Fact]
    public void ShrinkTable_CanInsertAfterShrink()
    {
        _engine.ExecuteOne("create database", "testdb");
        _engine.ExecuteOne("create table users (name string 100)", "testdb");

        for (int i = 0; i < 5; i++)
            _engine.ExecuteOne($"upsert users {{name: 'user{i}'}}", "testdb");

        _engine.ExecuteOne("shrink table users chunk_size 100", "testdb");

        // Insert more rows after shrink
        for (int i = 5; i < 10; i++)
            _engine.ExecuteOne($"upsert users {{name: 'user{i}'}}", "testdb");

        var r = _engine.ExecuteOne("get users", "testdb");
        Assert.Equal(10, r.Data?.Count);
    }

    [Fact]
    public void ShrinkTable_UnknownTable_Error()
    {
        _engine.ExecuteOne("create database", "testdb");

        var r = _engine.ExecuteOne("shrink table nonexistent", "testdb");
        Assert.Equal(SproutOperation.Error, r.Operation);
    }

    // ── shrink database ──────────────────────────────────────

    [Fact]
    public void ShrinkDatabase_ShrinksAllTablesWithoutOwnChunkSize()
    {
        _engine.ExecuteOne("create database", "testdb");
        _engine.ExecuteOne("create table users (name string 100)", "testdb");
        _engine.ExecuteOne("create table logs (msg string 200)", "testdb");

        for (int i = 0; i < 3; i++)
        {
            _engine.ExecuteOne($"upsert users {{name: 'u{i}'}}", "testdb");
            _engine.ExecuteOne($"upsert logs {{msg: 'm{i}'}}", "testdb");
        }

        var r = _engine.ExecuteOne("shrink database chunk_size 200", "testdb");
        Assert.Equal(SproutOperation.ShrinkDatabase, r.Operation);
        Assert.Null(r.Errors);
        Assert.NotNull(r.Data);
        Assert.Equal(2, r.Data.Count);
    }

    [Fact]
    public void ShrinkDatabase_SkipsTablesWithOwnChunkSize()
    {
        _engine.ExecuteOne("create database", "testdb");
        _engine.ExecuteOne("create table users (name string) with chunk_size 150", "testdb");
        _engine.ExecuteOne("create table logs (msg string)", "testdb");

        _engine.ExecuteOne("upsert users {name: 'u1'}", "testdb");
        _engine.ExecuteOne("upsert logs {msg: 'm1'}", "testdb");

        var r = _engine.ExecuteOne("shrink database chunk_size 200", "testdb");
        Assert.NotNull(r.Data);

        var usersRow = r.Data.Find(d => d.ContainsKey("table") && (string)d["table"] == "users");
        Assert.NotNull(usersRow);
        Assert.True((bool)usersRow["skipped"]);

        var logsRow = r.Data.Find(d => d.ContainsKey("table") && (string)d["table"] == "logs");
        Assert.NotNull(logsRow);
        Assert.False(logsRow.ContainsKey("skipped"));
    }

    [Fact]
    public void ShrinkDatabase_UpdatesMetaChunkSize()
    {
        _engine.ExecuteOne("create database", "testdb");

        _engine.ExecuteOne("shrink database chunk_size 300", "testdb");

        var desc = _engine.ExecuteOne("describe", "testdb");
        Assert.Equal(300, desc.Schema?.ChunkSize);
    }

    [Fact]
    public void ShrinkDatabase_PreservesAllData()
    {
        _engine.ExecuteOne("create database", "testdb");
        _engine.ExecuteOne("create table users (name string 100)", "testdb");

        for (int i = 0; i < 10; i++)
            _engine.ExecuteOne($"upsert users {{name: 'user{i}'}}", "testdb");

        _engine.ExecuteOne("shrink database chunk_size 200", "testdb");

        var r = _engine.ExecuteOne("get users", "testdb");
        Assert.Equal(10, r.Data?.Count);
    }

    // ── Parser tests ─────────────────────────────────────────

    [Fact]
    public void Parse_ShrinkTable_Basic()
    {
        _engine.ExecuteOne("create database", "testdb");
        _engine.ExecuteOne("create table users (name string)", "testdb");

        var r = _engine.ExecuteOne("shrink table users", "testdb");
        Assert.Equal(SproutOperation.ShrinkTable, r.Operation);
    }

    [Fact]
    public void Parse_ShrinkTable_WithChunkSize()
    {
        _engine.ExecuteOne("create database", "testdb");
        _engine.ExecuteOne("create table users (name string)", "testdb");

        var r = _engine.ExecuteOne("shrink table users chunk_size 500", "testdb");
        Assert.Equal(SproutOperation.ShrinkTable, r.Operation);
    }

    [Fact]
    public void Parse_ShrinkDatabase_Basic()
    {
        _engine.ExecuteOne("create database", "testdb");
        var r = _engine.ExecuteOne("shrink database", "testdb");
        Assert.Equal(SproutOperation.ShrinkDatabase, r.Operation);
    }

    [Fact]
    public void Parse_ShrinkDatabase_WithChunkSize()
    {
        _engine.ExecuteOne("create database", "testdb");
        var r = _engine.ExecuteOne("shrink database chunk_size 500", "testdb");
        Assert.Equal(SproutOperation.ShrinkDatabase, r.Operation);
    }

    [Fact]
    public void Parse_Shrink_InvalidChunkSize_Error()
    {
        _engine.ExecuteOne("create database", "testdb");
        _engine.ExecuteOne("create table users (name string)", "testdb");

        var r = _engine.ExecuteOne("shrink table users chunk_size 50", "testdb");
        Assert.Equal(SproutOperation.Error, r.Operation);
    }

    [Fact]
    public void Parse_Shrink_MissingTarget_Error()
    {
        _engine.ExecuteOne("create database", "testdb");
        var r = _engine.ExecuteOne("shrink", "testdb");
        Assert.Equal(SproutOperation.Error, r.Operation);
    }

    // ── Data integrity: values in all columns correct after shrink ──

    [Fact]
    public void ShrinkTable_MultipleColumns_AllValuesPreserved()
    {
        _engine.ExecuteOne("create database", "testdb");
        _engine.ExecuteOne("create table items (name string 50, price uint, active bool)", "testdb");

        _engine.ExecuteOne("upsert items {name: 'apple', price: 150, active: true}", "testdb");
        _engine.ExecuteOne("upsert items {name: 'banana', price: 80, active: false}", "testdb");
        _engine.ExecuteOne("upsert items {name: 'cherry', price: 200, active: true}", "testdb");

        // Delete middle row
        _engine.ExecuteOne("delete items where _id = 2", "testdb");

        _engine.ExecuteOne("shrink table items chunk_size 100", "testdb");

        var r = _engine.ExecuteOne("get items order by _id", "testdb");
        Assert.NotNull(r.Data);
        Assert.Equal(2, r.Data.Count);

        Assert.Equal("apple", r.Data[0]["name"]);
        Assert.Equal((uint)150, r.Data[0]["price"]);
        Assert.Equal(true, r.Data[0]["active"]);

        Assert.Equal("cherry", r.Data[1]["name"]);
        Assert.Equal((uint)200, r.Data[1]["price"]);
        Assert.Equal(true, r.Data[1]["active"]);
    }

    [Fact]
    public void ShrinkTable_NullableColumn_PreservesNulls()
    {
        _engine.ExecuteOne("create database", "testdb");
        _engine.ExecuteOne("create table users (name string 50, email string 100)", "testdb");

        _engine.ExecuteOne("upsert users {name: 'alice'}", "testdb");
        _engine.ExecuteOne("upsert users {name: 'bob', email: 'bob@test.com'}", "testdb");

        _engine.ExecuteOne("shrink table users chunk_size 100", "testdb");

        var r = _engine.ExecuteOne("get users order by _id", "testdb");
        Assert.NotNull(r.Data);
        Assert.Equal(2, r.Data.Count);

        Assert.Equal("alice", r.Data[0]["name"]);
        Assert.Null(r.Data[0]["email"]);

        Assert.Equal("bob", r.Data[1]["name"]);
        Assert.Equal("bob@test.com", r.Data[1]["email"]);
    }

    // ── Custom engine ChunkSize ──────────────────────────────

    [Fact]
    public void CustomEngineChunkSize_UsedAsDefault()
    {
        var customDir = Path.Combine(Path.GetTempPath(), $"sproutdb-test-{Guid.NewGuid()}");
        try
        {
            using var customEngine = new SproutEngine(new SproutEngineSettings
            {
                DataDirectory = customDir,
                ChunkSize = 500,
            });

            customEngine.Execute("create database", "testdb");
            customEngine.Execute("create table users (name string 100)", "testdb");

            var indexPath = Path.Combine(customDir, "testdb", "users", "_index");
            Assert.Equal(20 + 500 * 8, new FileInfo(indexPath).Length);
        }
        finally
        {
            if (Directory.Exists(customDir))
                Directory.Delete(customDir, true);
        }
    }
}

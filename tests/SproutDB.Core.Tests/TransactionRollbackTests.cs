namespace SproutDB.Core.Tests;

/// <summary>
/// Rollback must also restore state that lives outside the column/index MMFs:
/// blob files, array files and TTL entries.
/// </summary>
public class TransactionRollbackTests : IDisposable
{
    private const string FailingStatement = "upsert nonexistent {x: 1}";

    private readonly string _tempDir;
    private readonly SproutEngine _engine;

    public TransactionRollbackTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), $"sproutdb-test-{Guid.NewGuid()}");
        _engine = new SproutEngine(_tempDir);
        _engine.ExecuteOne("create database", "testdb");
        _engine.ExecuteOne("create table files (name string 100, data blob, tags array string 30)", "testdb");
    }

    public void Dispose()
    {
        _engine.Dispose();
        if (Directory.Exists(_tempDir))
            Directory.Delete(_tempDir, true);
    }

    private static string B64(string text) => Convert.ToBase64String(System.Text.Encoding.UTF8.GetBytes(text));

    private void AssertRolledBack(string transaction)
    {
        var results = _engine.Execute(transaction, "testdb");
        var error = Assert.Single(results);
        Assert.Equal(SproutOperation.Error, error.Operation);
        Assert.StartsWith("transaction rolled back:", error.Errors?[0].Message ?? "");
    }

    private Dictionary<string, object?> GetFile(string name)
    {
        var r = _engine.ExecuteOne($"get files where name = '{name}'", "testdb");
        return Assert.Single(r.Data ?? []);
    }

    // ── Blob ─────────────────────────────────────────────────

    [Fact]
    public void Rollback_BlobUpdate_RestoresOldContent()
    {
        _engine.ExecuteOne($"upsert files {{name: 'a', data: '{B64("old")}'}}", "testdb");

        AssertRolledBack($"atomic; upsert files {{name: 'a', data: '{B64("new")}'}} on name; {FailingStatement}; commit");

        Assert.Equal(B64("old"), GetFile("a")["data"]);
    }

    [Fact]
    public void Rollback_BlobSetToNull_RestoresOldContent()
    {
        _engine.ExecuteOne($"upsert files {{name: 'a', data: '{B64("old")}'}}", "testdb");

        AssertRolledBack($"atomic; upsert files {{name: 'a', data: null}} on name; {FailingStatement}; commit");

        Assert.Equal(B64("old"), GetFile("a")["data"]);
    }

    [Fact]
    public void Rollback_BlobInsert_RemovesBlobFile()
    {
        AssertRolledBack($"atomic; upsert files {{name: 'a', data: '{B64("new")}'}}; {FailingStatement}; commit");

        // The rolled-back insert got _id 1 — its blob file must be gone,
        // otherwise the next row with _id 1 would inherit it.
        Assert.Empty(Directory.GetFiles(Path.Combine(_tempDir, "testdb", "files"), "*.blob"));

        _engine.ExecuteOne("upsert files {name: 'b'}", "testdb");
        Assert.Null(GetFile("b")["data"]);
    }

    [Fact]
    public void Rollback_DeleteRowWithBlob_RestoresBlob()
    {
        _engine.ExecuteOne($"upsert files {{name: 'a', data: '{B64("keep")}'}}", "testdb");

        AssertRolledBack($"atomic; delete files where name = 'a'; {FailingStatement}; commit");

        Assert.Equal(B64("keep"), GetFile("a")["data"]);
    }

    // ── Array ────────────────────────────────────────────────

    [Fact]
    public void Rollback_ArrayUpdate_RestoresOldElements()
    {
        _engine.ExecuteOne("upsert files {name: 'a', tags: ['x', 'y']}", "testdb");

        AssertRolledBack($"atomic; upsert files {{name: 'a', tags: ['z']}} on name; {FailingStatement}; commit");

        var tags = GetFile("a")["tags"] as List<object?>;
        Assert.NotNull(tags);
        Assert.Equal(["x", "y"], tags.Select(t => t?.ToString()));
    }

    [Fact]
    public void Rollback_DeleteRowWithArray_RestoresArray()
    {
        _engine.ExecuteOne("upsert files {name: 'a', tags: ['x']}", "testdb");

        AssertRolledBack($"atomic; delete files where name = 'a'; {FailingStatement}; commit");

        var tags = GetFile("a")["tags"] as List<object?>;
        Assert.NotNull(tags);
        Assert.Equal(["x"], tags.Select(t => t?.ToString()));
    }

    // ── TTL ──────────────────────────────────────────────────

    [Fact]
    public void Rollback_RowTtlUpdate_RestoresOldTtl()
    {
        _engine.ExecuteOne("upsert files {name: 'a', ttl: 1h}", "testdb");

        AssertRolledBack($"atomic; upsert files {{name: 'a', ttl: 2d}} on name; {FailingStatement}; commit");

        var r = _engine.ExecuteOne("get files select name, _ttl where name = 'a'", "testdb");
        var row = Assert.Single(r.Data ?? []);
        Assert.Equal(3600L, row["_ttl"]);
    }

    [Fact]
    public void Rollback_DeleteRowWithTtl_RestoresTtl()
    {
        _engine.ExecuteOne("upsert files {name: 'a', ttl: 1h}", "testdb");

        AssertRolledBack($"atomic; delete files where name = 'a'; {FailingStatement}; commit");

        var r = _engine.ExecuteOne("get files select name, _ttl, _expiresat where name = 'a'", "testdb");
        var row = Assert.Single(r.Data ?? []);
        Assert.Equal(3600L, row["_ttl"]);
        Assert.NotEqual(0L, row["_expiresat"]);
    }
}

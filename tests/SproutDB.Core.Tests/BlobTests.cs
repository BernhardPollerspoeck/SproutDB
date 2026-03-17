namespace SproutDB.Core.Tests;

public class BlobTests : IDisposable
{
    private readonly string _tempDir;
    private SproutEngine _engine;
    private bool _disposed;

    public BlobTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), $"sproutdb-test-{Guid.NewGuid()}");
        _engine = new SproutEngine(_tempDir);
        _engine.ExecuteOne("create database", "testdb");
        _engine.ExecuteOne("create table files (name string 100, data blob)", "testdb");
    }

    public void Dispose()
    {
        if (!_disposed)
        {
            _disposed = true;
            _engine.Dispose();
        }
        if (Directory.Exists(_tempDir))
            Directory.Delete(_tempDir, true);
    }

    // ── Create table with blob column ──────────────────────

    [Fact]
    public void CreateTable_WithBlobColumn_Succeeds()
    {
        var result = _engine.ExecuteOne("describe files", "testdb");
        Assert.Equal(SproutOperation.Describe, result.Operation);

        var columns = result.Schema?.Columns;
        Assert.NotNull(columns);
        var blobCol = columns.FirstOrDefault(c => c.Name == "data");
        Assert.NotNull(blobCol);
        Assert.Equal("blob", blobCol.Type);
    }

    [Fact]
    public void AddColumn_Blob_Succeeds()
    {
        _engine.ExecuteOne("add column files.thumbnail blob", "testdb");
        var result = _engine.ExecuteOne("describe files", "testdb");
        var cols = result.Schema?.Columns;
        Assert.NotNull(cols);
        Assert.Contains(cols, c => c.Name == "thumbnail" && c.Type == "blob");
    }

    // ── Insert and read blob ──────────────────────────────

    [Fact]
    public void Upsert_BlobColumn_StoresAndReturnsBase64()
    {
        var original = "Hello, Blob World!"u8.ToArray();
        var base64 = Convert.ToBase64String(original);

        var upsertResult = _engine.ExecuteOne($"upsert files {{ name: 'test.txt', data: '{base64}' }}", "testdb");
        Assert.Equal(SproutOperation.Upsert, upsertResult.Operation);
        Assert.Equal(1, upsertResult.Affected);

        // Upsert response returns byte count for blob
        var record = upsertResult.Data![0];
        Assert.Equal((long)original.Length, record["data"]);

        // GET returns actual base64 data
        var getResult = _engine.ExecuteOne("get files", "testdb");
        Assert.Single(getResult.Data!);
        Assert.Equal(base64, getResult.Data[0]["data"]);
    }

    [Fact]
    public void Upsert_BlobColumn_Null_Succeeds()
    {
        var result = _engine.ExecuteOne("upsert files { name: 'empty.txt', data: null }", "testdb");
        Assert.Equal(SproutOperation.Upsert, result.Operation);

        var getResult = _engine.ExecuteOne("get files", "testdb");
        Assert.Null(getResult.Data![0]["data"]);
    }

    [Fact]
    public void Upsert_BlobColumn_UpdateExisting_OverwritesFile()
    {
        var data1 = Convert.ToBase64String("version1"u8.ToArray());
        var data2 = Convert.ToBase64String("version2-longer"u8.ToArray());

        _engine.ExecuteOne($"upsert files {{ name: 'doc.txt', data: '{data1}' }}", "testdb");
        _engine.ExecuteOne($"upsert files {{ _id: 1, name: 'doc.txt', data: '{data2}' }}", "testdb");

        var getResult = _engine.ExecuteOne("get files", "testdb");
        Assert.Equal(data2, getResult.Data![0]["data"]);
    }

    [Fact]
    public void Upsert_BlobColumn_SetToNull_DeletesFile()
    {
        var data = Convert.ToBase64String("somedata"u8.ToArray());
        _engine.ExecuteOne($"upsert files {{ name: 'doc.txt', data: '{data}' }}", "testdb");

        // Update to null
        _engine.ExecuteOne("upsert files { _id: 1, data: null }", "testdb");

        var getResult = _engine.ExecuteOne("get files", "testdb");
        Assert.Null(getResult.Data![0]["data"]);

        // Verify .blob file is deleted
        var tablePath = Path.Combine(_tempDir, "testdb", "files");
        Assert.False(File.Exists(Path.Combine(tablePath, "data_1.blob")));
    }

    // ── Delete ─────────────────────────────────────────────

    [Fact]
    public void Delete_Row_DeletesBlobFile()
    {
        var data = Convert.ToBase64String("deleteMe"u8.ToArray());
        _engine.ExecuteOne($"upsert files {{ name: 'temp.txt', data: '{data}' }}", "testdb");

        var tablePath = Path.Combine(_tempDir, "testdb", "files");
        Assert.True(File.Exists(Path.Combine(tablePath, "data_1.blob")));

        _engine.ExecuteOne("delete files where name = 'temp.txt'", "testdb");

        Assert.False(File.Exists(Path.Combine(tablePath, "data_1.blob")));
    }

    // ── Select ─────────────────────────────────────────────

    [Fact]
    public void Get_WithSelect_BlobColumn_Works()
    {
        var data = Convert.ToBase64String("selecttest"u8.ToArray());
        _engine.ExecuteOne($"upsert files {{ name: 'sel.txt', data: '{data}' }}", "testdb");

        // Select only name (exclude blob)
        var result = _engine.ExecuteOne("get files select name", "testdb");
        Assert.Single(result.Data!);
        Assert.False(result.Data[0].ContainsKey("data"));

        // Select only blob
        var result2 = _engine.ExecuteOne("get files select data", "testdb");
        Assert.Equal(data, result2.Data![0]["data"]);
    }

    // ── Type validation ────────────────────────────────────

    [Fact]
    public void Upsert_BlobColumn_NonStringValue_TypeError()
    {
        var result = _engine.ExecuteOne("upsert files { name: 'bad.txt', data: 42 }", "testdb");
        Assert.Equal(SproutOperation.Error, result.Operation);
        Assert.Contains("type mismatch", result.Errors![0].Message);
    }

    [Fact]
    public void Upsert_BlobColumn_InvalidBase64_ReturnsTypeMismatch()
    {
        var result = _engine.ExecuteOne("upsert files { name: 'bad.txt', data: 'this is plain text, not base64' }", "testdb");
        Assert.Equal(SproutOperation.Error, result.Operation);
        Assert.Contains("not valid base64", result.Errors![0].Message);
    }

    [Fact]
    public void Upsert_BlobColumn_InvalidBase64_DoesNotCrashWalReplay()
    {
        _engine.ExecuteOne("upsert files { name: 'bad.txt', data: 'not base64!!!' }", "testdb");

        // Reload engine — WAL replay must not crash
        _engine.Dispose();
        _engine = new SproutEngine(_tempDir);

        // Engine should be functional
        var result = _engine.ExecuteOne("get files", "testdb");
        Assert.Empty(result.Data!);
    }

    // ── Index restriction ──────────────────────────────────

    [Fact]
    public void CreateIndex_OnBlobColumn_Fails()
    {
        var result = _engine.ExecuteOne("create index files.data", "testdb");
        Assert.Equal(SproutOperation.Error, result.Operation);
        Assert.Contains("blob", result.Errors![0].Message);
    }

    [Fact]
    public void CreateUniqueIndex_OnBlobColumn_Fails()
    {
        var result = _engine.ExecuteOne("create index unique files.data", "testdb");
        Assert.Equal(SproutOperation.Error, result.Operation);
        Assert.Contains("blob", result.Errors![0].Message);
    }

    // ── Blob file naming ───────────────────────────────────

    [Fact]
    public void BlobFile_NamedCorrectly()
    {
        var data = Convert.ToBase64String("filecheck"u8.ToArray());
        _engine.ExecuteOne($"upsert files {{ name: 'check.txt', data: '{data}' }}", "testdb");

        var tablePath = Path.Combine(_tempDir, "testdb", "files");
        Assert.True(File.Exists(Path.Combine(tablePath, "data_1.blob")));

        // Verify raw content matches
        var rawBytes = File.ReadAllBytes(Path.Combine(tablePath, "data_1.blob"));
        Assert.Equal("filecheck"u8.ToArray(), rawBytes);
    }

    // ── Multiple blob columns ──────────────────────────────

    [Fact]
    public void MultipleBlobColumns_IndependentFiles()
    {
        _engine.ExecuteOne("create table docs (title string 100, content blob, preview blob)", "testdb");

        var content = Convert.ToBase64String("full document"u8.ToArray());
        var preview = Convert.ToBase64String("thumb"u8.ToArray());

        _engine.ExecuteOne($"upsert docs {{ title: 'doc1', content: '{content}', preview: '{preview}' }}", "testdb");

        var result = _engine.ExecuteOne("get docs", "testdb");
        Assert.Equal(content, result.Data![0]["content"]);
        Assert.Equal(preview, result.Data[0]["preview"]);

        var tablePath = Path.Combine(_tempDir, "testdb", "docs");
        Assert.True(File.Exists(Path.Combine(tablePath, "content_1.blob")));
        Assert.True(File.Exists(Path.Combine(tablePath, "preview_1.blob")));
    }

    // ── Persistence ────────────────────────────────────────

    [Fact]
    public void BlobData_PersistsAfterReload()
    {
        var data = Convert.ToBase64String("persistent data"u8.ToArray());
        _engine.ExecuteOne($"upsert files {{ name: 'persist.txt', data: '{data}' }}", "testdb");

        // Reload engine
        _engine.Dispose();
        _engine = new SproutEngine(_tempDir);

        var result = _engine.ExecuteOne("get files", "testdb");
        Assert.Single(result.Data!);
        Assert.Equal(data, result.Data[0]["data"]);
    }
}

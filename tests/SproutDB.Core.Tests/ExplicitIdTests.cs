namespace SproutDB.Core.Tests;

public class ExplicitIdTests : IDisposable
{
    private readonly string _tempDir;
    private readonly SproutEngine _engine;

    public ExplicitIdTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), $"sproutdb-test-{Guid.NewGuid()}");
        _engine = new SproutEngine(_tempDir);
        _engine.Execute("create database", "testdb");
        _engine.Execute("create table users (name string 100, age ubyte)", "testdb");
    }

    public void Dispose()
    {
        _engine.Dispose();
        if (Directory.Exists(_tempDir))
            Directory.Delete(_tempDir, true);
    }

    // ── Explicit _id that exists → update works ──────────

    [Fact]
    public void Upsert_ExplicitId_ExistingRow_Updates()
    {
        var insert = _engine.Execute("upsert users {name: 'Alice', age: 25}", "testdb");
        var id = insert.Data?[0]["_id"];
        Assert.NotNull(id);

        var update = _engine.Execute($"upsert users {{_id: {id}, name: 'Alice Updated', age: 30}}", "testdb");
        Assert.Equal(SproutOperation.Upsert, update.Operation);
        Assert.Equal(1, update.Affected);
        Assert.Equal("Alice Updated", update.Data?[0]["name"]?.ToString());
    }

    // ── Explicit _id that does NOT exist → error ─────────

    [Fact]
    public void Upsert_ExplicitId_NonExistent_ReturnsError()
    {
        var result = _engine.Execute("upsert users {_id: 999, name: 'Ghost'}", "testdb");
        Assert.Equal(SproutOperation.Error, result.Operation);
        Assert.NotNull(result.Errors);
        Assert.Single(result.Errors);
        Assert.Equal("ID_NOT_FOUND", result.Errors[0].Code);
        Assert.Contains("999", result.Errors[0].Message);
    }

    [Fact]
    public void Upsert_ExplicitId_LargeNonExistent_ReturnsError()
    {
        var result = _engine.Execute("upsert users {_id: 999999, name: 'BigId'}", "testdb");
        Assert.Equal(SproutOperation.Error, result.Operation);
        Assert.Equal("ID_NOT_FOUND", result.Errors?[0].Code);
    }

    // ── Without _id → normal insert (still works) ────────

    [Fact]
    public void Upsert_WithoutId_CreatesNewRow()
    {
        var result = _engine.Execute("upsert users {name: 'NewUser', age: 20}", "testdb");
        Assert.Equal(SproutOperation.Upsert, result.Operation);
        Assert.Equal(1, result.Affected);
        Assert.NotNull(result.Data?[0]["_id"]);
    }

    // ── Multiple records: one with bad _id fails entire batch ──

    [Fact]
    public void Upsert_ExplicitId_NonExistent_DoesNotAffectExistingRows()
    {
        var insert = _engine.Execute("upsert users {name: 'Original', age: 10}", "testdb");
        var id = insert.Data?[0]["_id"];
        Assert.NotNull(id);

        // Try to update a non-existent id — should fail
        var result = _engine.Execute("upsert users {_id: 888, name: 'Ghost'}", "testdb");
        Assert.Equal(SproutOperation.Error, result.Operation);
        Assert.Equal("ID_NOT_FOUND", result.Errors?[0].Code);

        // Verify original row is untouched
        var check = _engine.Execute($"get users where _id = {id}", "testdb");
        Assert.Equal("Original", check.Data?[0]["name"]?.ToString());
    }

    // ── Deleted row _id → error ──────────────────────────

    [Fact]
    public void Upsert_ExplicitId_DeletedRow_ReturnsError()
    {
        var insert = _engine.Execute("upsert users {name: 'ToDelete', age: 1}", "testdb");
        var id = insert.Data?[0]["_id"];
        Assert.NotNull(id);

        _engine.Execute($"delete users where _id = {id}", "testdb");

        var result = _engine.Execute($"upsert users {{_id: {id}, name: 'Revive'}}", "testdb");
        Assert.Equal(SproutOperation.Error, result.Operation);
        Assert.Equal("ID_NOT_FOUND", result.Errors?[0].Code);
    }
}

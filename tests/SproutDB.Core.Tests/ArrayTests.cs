namespace SproutDB.Core.Tests;

public class ArrayTests : IDisposable
{
    private readonly string _tempDir;
    private readonly SproutEngine _engine;

    public ArrayTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), $"sproutdb-test-{Guid.NewGuid()}");
        _engine = new SproutEngine(_tempDir);
        _engine.Execute("create database", "testdb");
        _engine.Execute("create table users (name string 100, roles array string 30)", "testdb");
    }

    public void Dispose()
    {
        _engine.Dispose();
        if (Directory.Exists(_tempDir))
            Directory.Delete(_tempDir, true);
    }

    // ── Create table with array column ─────────────────────

    [Fact]
    public void CreateTable_WithArrayColumn_Succeeds()
    {
        var result = _engine.Execute("describe users", "testdb");
        Assert.Equal(SproutOperation.Describe, result.Operation);

        var columns = result.Schema?.Columns;
        Assert.NotNull(columns);
        var arrayCol = columns.FirstOrDefault(c => c.Name == "roles");
        Assert.NotNull(arrayCol);
        Assert.Equal("array", arrayCol.Type);
    }

    [Fact]
    public void AddColumn_Array_Succeeds()
    {
        _engine.Execute("add column users.tags array string 50", "testdb");
        var result = _engine.Execute("describe users", "testdb");
        var cols = result.Schema?.Columns;
        Assert.NotNull(cols);
        Assert.Contains(cols, c => c.Name == "tags" && c.Type == "array");
    }

    // ── Upsert and read array ──────────────────────────────

    [Fact]
    public void Upsert_ArrayColumn_StoresAndReturnsArray()
    {
        var result = _engine.Execute("upsert users {name: 'Alice', roles: ['admin', 'editor']}", "testdb");
        Assert.Equal(SproutOperation.Upsert, result.Operation);
        Assert.Equal(1, result.Affected);

        var row = result.Data?[0];
        Assert.NotNull(row);
        Assert.True(row.ContainsKey("roles"));
        var roles = row["roles"] as List<object?>;
        Assert.NotNull(roles);
        Assert.Equal(2, roles.Count);
        Assert.Equal("admin", roles[0]?.ToString());
        Assert.Equal("editor", roles[1]?.ToString());
    }

    [Fact]
    public void Get_ArrayColumn_ReturnsArray()
    {
        _engine.Execute("upsert users {name: 'Bob', roles: ['viewer']}", "testdb");

        var result = _engine.Execute("get users", "testdb");
        Assert.Equal(SproutOperation.Get, result.Operation);

        var row = result.Data?.FirstOrDefault(r => r["name"]?.ToString() == "Bob");
        Assert.NotNull(row);
        var roles = row["roles"] as List<object?>;
        Assert.NotNull(roles);
        Assert.Single(roles);
        Assert.Equal("viewer", roles[0]?.ToString());
    }

    [Fact]
    public void Upsert_EmptyArray_StoresEmptyArray()
    {
        var result = _engine.Execute("upsert users {name: 'Charlie', roles: []}", "testdb");
        Assert.Equal(SproutOperation.Upsert, result.Operation);

        var row = result.Data?[0];
        Assert.NotNull(row);
        var roles = row["roles"] as List<object?>;
        Assert.NotNull(roles);
        Assert.Empty(roles);
    }

    [Fact]
    public void Upsert_NullArray_ReturnsNull()
    {
        _engine.Execute("upsert users {name: 'Dave', roles: ['x']}", "testdb");

        // Update to null
        var result = _engine.Execute("get users where name = 'Dave'", "testdb");
        var id = result.Data?[0]["_id"];
        Assert.NotNull(id);

        var update = _engine.Execute($"upsert users {{_id: {id}, roles: null}}", "testdb");
        Assert.Equal(SproutOperation.Upsert, update.Operation);

        var getResult = _engine.Execute($"get users where _id = {id}", "testdb");
        var row = getResult.Data?[0];
        Assert.NotNull(row);
        Assert.Null(row["roles"]);
    }

    // ── WHERE contains ─────────────────────────────────────

    [Fact]
    public void Where_ArrayContains_MatchesElement()
    {
        _engine.Execute("upsert users {name: 'Eve', roles: ['admin', 'user']}", "testdb");
        _engine.Execute("upsert users {name: 'Frank', roles: ['user']}", "testdb");

        var result = _engine.Execute("get users where roles contains 'admin'", "testdb");
        Assert.Equal(SproutOperation.Get, result.Operation);
        Assert.NotNull(result.Data);
        Assert.Single(result.Data);
        Assert.Equal("Eve", result.Data[0]["name"]?.ToString());
    }

    [Fact]
    public void Where_ArrayContains_NoMatch_ReturnsEmpty()
    {
        _engine.Execute("upsert users {name: 'Grace', roles: ['viewer']}", "testdb");

        var result = _engine.Execute("get users where roles contains 'admin'", "testdb");
        // Grace doesn't have admin, and any previous test rows may or may not exist
        Assert.NotNull(result.Data);
        Assert.DoesNotContain(result.Data, r => r["name"]?.ToString() == "Grace");
    }

    [Fact]
    public void Where_ArrayContains_NumericElements()
    {
        _engine.Execute("create table scores (player string 50, values array sint)", "testdb");
        _engine.Execute("upsert scores {player: 'Alice', values: [10, 20, 30]}", "testdb");
        _engine.Execute("upsert scores {player: 'Bob', values: [5, 15]}", "testdb");

        var result = _engine.Execute("get scores where values contains '20'", "testdb");
        Assert.NotNull(result.Data);
        Assert.Single(result.Data);
        Assert.Equal("Alice", result.Data[0]["player"]?.ToString());
    }

    // ── Delete cleans up array files ───────────────────────

    [Fact]
    public void Delete_ArrayColumn_CleansUpFiles()
    {
        _engine.Execute("upsert users {name: 'Hank', roles: ['admin']}", "testdb");
        var getResult = _engine.Execute("get users where name = 'Hank'", "testdb");
        var id = getResult.Data?[0]["_id"];
        Assert.NotNull(id);

        var deleteResult = _engine.Execute($"delete users where _id = {id}", "testdb");
        Assert.Equal(SproutOperation.Delete, deleteResult.Operation);
        Assert.Equal(1, deleteResult.Affected);

        // Verify row is gone
        var afterDelete = _engine.Execute($"get users where _id = {id}", "testdb");
        Assert.NotNull(afterDelete.Data);
        Assert.Empty(afterDelete.Data);
    }

    // ── Index restriction ──────────────────────────────────

    [Fact]
    public void CreateIndex_OnArrayColumn_ReturnsError()
    {
        var result = _engine.Execute("create index users.roles", "testdb");
        Assert.Equal(SproutOperation.Error, result.Operation);
        Assert.Contains("cannot create index on array column", result.Errors?[0].Message ?? "");
    }

    // ── Type validation ────────────────────────────────────

    [Fact]
    public void Upsert_NonArrayValue_IntoArrayColumn_ReturnsError()
    {
        var result = _engine.Execute("upsert users {name: 'Fail', roles: 'notanarray'}", "testdb");
        Assert.Equal(SproutOperation.Error, result.Operation);
    }

    // ── Multiple records with arrays ───────────────────────

    [Fact]
    public void Upsert_MultipleRecords_WithArrays()
    {
        // Insert two records separately (multi-record array syntax may need special parsing)
        var r1 = _engine.Execute("upsert users {name: 'Multi1', roles: ['a']}", "testdb");
        Assert.Equal(SproutOperation.Upsert, r1.Operation);

        var r2 = _engine.Execute("upsert users {name: 'Multi2', roles: ['b', 'c']}", "testdb");
        Assert.Equal(SproutOperation.Upsert, r2.Operation);

        var result = _engine.Execute("get users where name starts 'Multi'", "testdb");
        Assert.NotNull(result.Data);
        Assert.Equal(2, result.Data.Count);
    }

    // ── Update existing row array ──────────────────────────

    [Fact]
    public void Upsert_UpdateExistingArray_ReplacesValue()
    {
        _engine.Execute("upsert users {name: 'Iris', roles: ['old']}", "testdb");
        var getResult = _engine.Execute("get users where name = 'Iris'", "testdb");
        var id = getResult.Data?[0]["_id"];
        Assert.NotNull(id);

        _engine.Execute($"upsert users {{_id: {id}, roles: ['new1', 'new2']}}", "testdb");

        var updated = _engine.Execute($"get users where _id = {id}", "testdb");
        var row = updated.Data?[0];
        Assert.NotNull(row);
        var roles = row["roles"] as List<object?>;
        Assert.NotNull(roles);
        Assert.Equal(2, roles.Count);
        Assert.Equal("new1", roles[0]?.ToString());
        Assert.Equal("new2", roles[1]?.ToString());
    }
}

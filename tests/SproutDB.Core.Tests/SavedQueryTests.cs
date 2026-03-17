namespace SproutDB.Core.Tests;

public class SavedQueryTests : IDisposable
{
    private readonly string _tempDir;
    private readonly SproutEngine _engine;
    private const string Db = "testdb";

    public SavedQueryTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), $"sproutdb-test-{Guid.NewGuid()}");
        _engine = new SproutEngine(_tempDir);
        _engine.ExecuteOne("create database", Db);
    }

    public void Dispose()
    {
        _engine.Dispose();
        if (Directory.Exists(_tempDir))
            Directory.Delete(_tempDir, true);
    }

    [Fact]
    public void CreateSavedQueriesTable()
    {
        var r = _engine.ExecuteInternal("create table _saved_queries (name string 200, query string 4000, pinned bool)", Db);
        Assert.True(r.Operation != SproutOperation.Error,
            $"Error: {string.Join("; ", r.Errors?.Select(e => $"{e.Code}: {e.Message}") ?? [])}");
    }

    [Fact]
    public void UpsertSavedQuery()
    {
        _engine.ExecuteInternal("create table _saved_queries (name string 200, query string 4000, pinned bool)", Db);

        var r = _engine.ExecuteInternal("upsert _saved_queries { name: 'test query', query: 'get users', pinned: false } on name", Db);
        Assert.NotEqual(SproutOperation.Error, r.Operation);

        var get = _engine.ExecuteInternal("get _saved_queries", Db);
        Assert.Equal(1, get.Affected);
        Assert.Equal("test query", get.Data?[0]["name"]?.ToString());
        Assert.Equal("get users", get.Data?[0]["query"]?.ToString());
    }

    [Fact]
    public void UpsertSavedQuery_WithComplexQuery()
    {
        _engine.ExecuteInternal("create table _saved_queries (name string 200, query string 4000, pinned bool)", Db);

        var complexQuery = "get customers select name, city where city = 'München' or city = 'Berlin' order by name asc";
        var escapedQuery = complexQuery.Replace("'", "\\'");

        var upsertQuery = $"upsert _saved_queries {{ name: 'complex', query: '{escapedQuery}', pinned: false }} on name";
        var r = _engine.ExecuteInternal(upsertQuery, Db);

        Assert.True(r.Operation != SproutOperation.Error,
            $"Error: {string.Join("; ", r.Errors?.Select(e => $"{e.Code}: {e.Message}") ?? [])}\nQuery: {upsertQuery}");

        var get = _engine.ExecuteInternal("get _saved_queries", Db);
        Assert.Equal(1, get.Affected);
    }

    [Fact]
    public void UpsertSavedQuery_WithSimpleQuotedQuery()
    {
        _engine.ExecuteInternal("create table _saved_queries (name string 200, query string 4000, pinned bool)", Db);

        // Test with a simpler quoted query
        var simpleQuery = "get users where name = 'Alice'";
        var escapedQuery = simpleQuery.Replace("'", "\\'");

        var upsertQuery = $"upsert _saved_queries {{ name: 'simple', query: '{escapedQuery}', pinned: false }} on name";
        var r = _engine.ExecuteInternal(upsertQuery, Db);

        Assert.True(r.Operation != SproutOperation.Error,
            $"Error: {string.Join("; ", r.Errors?.Select(e => $"{e.Code}: {e.Message}") ?? [])}\nQuery: {upsertQuery}");
    }

    [Fact]
    public void DescribeNonExistentTable_ReturnsError()
    {
        var r = _engine.ExecuteInternal("describe _saved_queries", Db);
        Assert.Equal(SproutOperation.Error, r.Operation);
    }

    [Fact]
    public void PinSavedQuery()
    {
        _engine.ExecuteInternal("create table _saved_queries (name string 200, query string 4000, pinned bool)", Db);
        _engine.ExecuteInternal("upsert _saved_queries { name: 'test', query: 'get users', pinned: false } on name", Db);

        var r = _engine.ExecuteInternal("upsert _saved_queries { name: 'test', pinned: true } on name", Db);
        Assert.NotEqual(SproutOperation.Error, r.Operation);

        var get = _engine.ExecuteInternal("get _saved_queries where name = 'test'", Db);
        Assert.Equal(true, get.Data?[0]["pinned"]);
    }

    [Fact]
    public void DeleteSavedQuery()
    {
        _engine.ExecuteInternal("create table _saved_queries (name string 200, query string 4000, pinned bool)", Db);
        _engine.ExecuteInternal("upsert _saved_queries { name: 'test', query: 'get users', pinned: false } on name", Db);

        var r = _engine.ExecuteInternal("delete _saved_queries where name = 'test'", Db);
        Assert.NotEqual(SproutOperation.Error, r.Operation);

        var get = _engine.ExecuteInternal("get _saved_queries", Db);
        Assert.Equal(0, get.Affected);
    }
}

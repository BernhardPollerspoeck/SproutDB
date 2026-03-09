namespace SproutDB.Core.Tests;

public class InProcessChangeTests : IDisposable
{
    private readonly string _tempDir;
    private readonly SproutEngine _engine;

    public InProcessChangeTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), $"sproutdb-test-{Guid.NewGuid()}");
        _engine = new SproutEngine(_tempDir);
        _engine.Execute("create database", "testdb");
        _engine.Execute(
            "create table users (name string 100, email string 200)",
            "testdb");

        // Wait for any pending change events from setup to be dispatched
        // before tests subscribe their callbacks.
        Thread.Sleep(100);
    }

    public void Dispose()
    {
        _engine.Dispose();
        if (Directory.Exists(_tempDir))
            Directory.Delete(_tempDir, true);
    }

    [Fact]
    public void OnChange_UpsertFiresCallback()
    {
        var db = _engine.SelectDatabase("testdb");
        var received = new List<SproutResponse>();
        db.OnChange("users", r => received.Add(r));

        _engine.Execute("upsert users {name: 'John', email: 'john@test.com'}", "testdb");

        WaitForDispatch();

        Assert.Single(received);
        Assert.Equal(SproutOperation.Upsert, received[0].Operation);
        Assert.Equal(1, received[0].Affected);
    }

    [Fact]
    public void OnChange_DeleteFiresCallback()
    {
        var db = _engine.SelectDatabase("testdb");
        _engine.Execute("upsert users {name: 'John', email: 'john@test.com'}", "testdb");
        WaitForDispatch(); // let upsert event drain before subscribing

        var received = new List<SproutResponse>();
        db.OnChange("users", r => received.Add(r));

        _engine.Execute("delete users where _id = 1", "testdb");

        WaitForDispatch();

        Assert.Single(received);
        Assert.Equal(SproutOperation.Delete, received[0].Operation);
    }

    [Fact]
    public void OnChange_ReadDoesNotFireCallback()
    {
        var db = _engine.SelectDatabase("testdb");
        var received = new List<SproutResponse>();
        db.OnChange("users", r => received.Add(r));

        _engine.Execute("get users", "testdb");

        WaitForDispatch();

        Assert.Empty(received);
    }

    [Fact]
    public void OnChange_SchemaChangeFiresSchemaCallback()
    {
        var db = _engine.SelectDatabase("testdb");
        var received = new List<SproutResponse>();
        db.OnChange("_schema", r => received.Add(r));

        _engine.Execute(
            "create table orders (product string 100)",
            "testdb");

        WaitForDispatch();

        Assert.Single(received);
        Assert.Equal(SproutOperation.CreateTable, received[0].Operation);
    }

    [Fact]
    public void OnChange_SchemaChangeAlsoFiresTableCallback()
    {
        var db = _engine.SelectDatabase("testdb");
        var tableReceived = new List<SproutResponse>();
        var schemaReceived = new List<SproutResponse>();
        db.OnChange("users", r => tableReceived.Add(r));
        db.OnChange("_schema", r => schemaReceived.Add(r));

        _engine.Execute("add column users.score sint", "testdb");

        WaitForDispatch();

        // Both the table callback and the _schema callback should fire
        Assert.Single(tableReceived);
        Assert.Single(schemaReceived);
    }

    [Fact]
    public void OnChange_UnsubscribeStopsCallbacks()
    {
        var db = _engine.SelectDatabase("testdb");
        var received = new List<SproutResponse>();
        var sub = db.OnChange("users", r => received.Add(r));

        _engine.Execute("upsert users {name: 'John', email: 'john@test.com'}", "testdb");
        WaitForDispatch();

        sub.Dispose();

        _engine.Execute("upsert users {name: 'Jane', email: 'jane@test.com'}", "testdb");
        WaitForDispatch();

        Assert.Single(received);
    }

    [Fact]
    public void OnChange_DifferentDatabaseDoesNotFire()
    {
        _engine.Execute("create database", "otherdb");
        _engine.Execute("create table users (name string 100)", "otherdb");

        var db = _engine.SelectDatabase("testdb");
        var received = new List<SproutResponse>();
        db.OnChange("users", r => received.Add(r));

        _engine.Execute("upsert users {name: 'John'}", "otherdb");

        WaitForDispatch();

        Assert.Empty(received);
    }

    [Fact]
    public void OnChange_ErrorDoesNotFire()
    {
        var db = _engine.SelectDatabase("testdb");
        var received = new List<SproutResponse>();
        db.OnChange("users", r => received.Add(r));

        // Upsert to non-existent table → should error, no notification
        _engine.Execute("upsert nonexistent {name: 'John'}", "testdb");

        WaitForDispatch();

        Assert.Empty(received);
    }

    [Fact]
    public void OnChange_CreateDatabaseFiresSchemaOnNewDb()
    {
        var received = new List<SproutResponse>();
        _engine.ChangeNotifier.Subscribe("newdb", "_schema", r => received.Add(r));

        _engine.Execute("create database", "newdb");

        WaitForDispatch();

        Assert.Single(received);
        Assert.Equal(SproutOperation.CreateDatabase, received[0].Operation);
    }

    private static void WaitForDispatch()
    {
        Thread.Sleep(100);
    }
}

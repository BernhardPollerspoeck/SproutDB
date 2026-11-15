namespace SproutDB.Core.Tests;

public class InProcessChangeTests : IDisposable
{
    private readonly string _tempDir;
    private readonly SproutEngine _engine;

    public InProcessChangeTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), $"sproutdb-test-{Guid.NewGuid()}");
        _engine = new SproutEngine(_tempDir);
        _engine.ExecuteOne("create database", "testdb");
        _engine.ExecuteOne(
            "create table users (name string 100, email string 200)",
            "testdb");

        // Let the change events from setup be dispatched before tests subscribe their callbacks.
        DrainChangeEvents();
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

        _engine.ExecuteOne("upsert users {name: 'John', email: 'john@test.com'}", "testdb");

        DrainChangeEvents();

        Assert.Single(received);
        Assert.Equal(SproutOperation.Upsert, received[0].Operation);
        Assert.Equal(1, received[0].Affected);
    }

    [Fact]
    public void OnChange_DeleteFiresCallback()
    {
        var db = _engine.SelectDatabase("testdb");
        _engine.ExecuteOne("upsert users {name: 'John', email: 'john@test.com'}", "testdb");
        DrainChangeEvents(); // let the upsert event be dispatched before subscribing

        var received = new List<SproutResponse>();
        db.OnChange("users", r => received.Add(r));

        _engine.ExecuteOne("delete users where _id = 1", "testdb");

        DrainChangeEvents();

        Assert.Single(received);
        Assert.Equal(SproutOperation.Delete, received[0].Operation);
    }

    [Fact]
    public void OnChange_ReadDoesNotFireCallback()
    {
        var db = _engine.SelectDatabase("testdb");
        var received = new List<SproutResponse>();
        db.OnChange("users", r => received.Add(r));

        _engine.ExecuteOne("get users", "testdb");

        DrainChangeEvents();

        Assert.Empty(received);
    }

    [Fact]
    public void OnChange_SchemaChangeFiresSchemaCallback()
    {
        var db = _engine.SelectDatabase("testdb");
        var received = new List<SproutResponse>();
        db.OnChange("_schema", r => received.Add(r));

        _engine.ExecuteOne(
            "create table orders (product string 100)",
            "testdb");

        DrainChangeEvents();

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

        _engine.ExecuteOne("add column users.score sint", "testdb");

        DrainChangeEvents();

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

        _engine.ExecuteOne("upsert users {name: 'John', email: 'john@test.com'}", "testdb");
        DrainChangeEvents();

        sub.Dispose();

        _engine.ExecuteOne("upsert users {name: 'Jane', email: 'jane@test.com'}", "testdb");
        DrainChangeEvents();

        Assert.Single(received);
    }

    [Fact]
    public void OnChange_DifferentDatabaseDoesNotFire()
    {
        _engine.ExecuteOne("create database", "otherdb");
        _engine.ExecuteOne("create table users (name string 100)", "otherdb");

        var db = _engine.SelectDatabase("testdb");
        var received = new List<SproutResponse>();
        db.OnChange("users", r => received.Add(r));

        _engine.ExecuteOne("upsert users {name: 'John'}", "otherdb");

        DrainChangeEvents();

        Assert.Empty(received);
    }

    [Fact]
    public void OnChange_ErrorDoesNotFire()
    {
        var db = _engine.SelectDatabase("testdb");
        var received = new List<SproutResponse>();
        db.OnChange("users", r => received.Add(r));

        // Upsert to non-existent table → should error, no notification
        _engine.ExecuteOne("upsert nonexistent {name: 'John'}", "testdb");

        DrainChangeEvents();

        Assert.Empty(received);
    }

    [Fact]
    public void OnChange_CreateDatabaseFiresSchemaOnNewDb()
    {
        var received = new List<SproutResponse>();
        _engine.ChangeNotifier.Subscribe("newdb", "_schema", r => received.Add(r));

        _engine.ExecuteOne("create database", "newdb");

        DrainChangeEvents();

        Assert.Single(received);
        Assert.Equal(SproutOperation.CreateDatabase, received[0].Operation);
    }

    /// <summary>
    /// Waits until every change event enqueued so far has been dispatched to the callbacks.
    /// The engine enqueues its events before a write returns, and the notifier dispatches
    /// them in order on a single loop: once a marker enqueued now has arrived, all earlier
    /// events have been delivered. A fixed sleep is not enough on machines with few cores,
    /// where the first dispatch can take far longer than 100 ms.
    /// </summary>
    private void DrainChangeEvents()
    {
        using var drained = new ManualResetEventSlim();
        using (_engine.ChangeNotifier.Subscribe("__drain", "__drain", _ => drained.Set()))
        {
            _engine.ChangeNotifier.Enqueue("__drain", "__drain", new SproutResponse { Operation = SproutOperation.Upsert });
            Assert.True(drained.Wait(TimeSpan.FromSeconds(10)), "change events were not dispatched within 10 s");
        }
    }
}

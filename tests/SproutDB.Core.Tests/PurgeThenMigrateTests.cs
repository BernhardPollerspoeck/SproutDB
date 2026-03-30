namespace SproutDB.Core.Tests;

/// <summary>
/// Reproduces the reset-endpoint scenario:
/// purge database → GetOrCreateDatabase → Migrate
/// </summary>
public class PurgeThenMigrateTests : IDisposable
{
    private readonly string _tempDir;
    private readonly SproutEngine _engine;

    public PurgeThenMigrateTests()
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

    [Fact]
    public void PurgeDatabase_ThenGetOrCreate_ThenMigrate_Works()
    {
        // Step 1: Create DB and run migrations (initial state)
        var db = _engine.GetOrCreateDatabase("testdb");
        _engine.Migrate(typeof(TestMigrations.Schema.CreateUsers).Assembly, db);

        // Verify initial state
        var desc = db.QueryOne("describe users");
        Assert.Equal(SproutOperation.Describe, desc.Operation);

        // Step 2: Purge the database (simulating reset endpoint)
        var purgeResult = db.QueryOne("purge database");
        Assert.Equal(SproutOperation.PurgeDatabase, purgeResult.Operation);
        Assert.Null(purgeResult.Errors);

        // Step 3: GetOrCreateDatabase (should create fresh)
        var freshDb = _engine.GetOrCreateDatabase("testdb");

        // Step 4: Re-run migrations on the fresh database
        _engine.Migrate(typeof(TestMigrations.Schema.CreateUsers).Assembly, freshDb);

        // Verify everything works
        var desc2 = freshDb.QueryOne("describe users");
        Assert.Equal(SproutOperation.Describe, desc2.Operation);
        Assert.NotNull(desc2.Schema?.Columns);
        Assert.Contains(desc2.Schema.Columns, c => c.Name == "name");
        Assert.Contains(desc2.Schema.Columns, c => c.Name == "email");

        // Verify _migrations table was populated
        var migrations = freshDb.QueryOne("get _migrations");
        Assert.Equal(2, migrations.Affected);
    }

    [Fact]
    public void PurgeDatabase_ThenGetOrCreate_CanCreateTablesAndUpsert()
    {
        // Step 1: Create and populate
        var db = _engine.GetOrCreateDatabase("testdb");
        db.QueryOne("create table items (name string 100)");
        db.QueryOne("upsert items {name: 'test'}");

        // Step 2: Purge
        db.QueryOne("purge database");

        // Step 3: GetOrCreate
        var freshDb = _engine.GetOrCreateDatabase("testdb");

        // Step 4: Create table, create index, upsert — the full migration pattern
        var createResult = freshDb.QueryOne("create table items (name string 100, price uint)");
        Assert.Equal(SproutOperation.CreateTable, createResult.Operation);
        Assert.Null(createResult.Errors);

        var indexResult = freshDb.QueryOne("create index items.name");
        Assert.Equal(SproutOperation.CreateIndex, indexResult.Operation);
        Assert.Null(indexResult.Errors);

        var upsertResult = freshDb.QueryOne("upsert items {name: 'hello', price: 42}");
        Assert.Equal(SproutOperation.Upsert, upsertResult.Operation);
        Assert.Null(upsertResult.Errors);

        var getResult = freshDb.QueryOne("get items");
        Assert.Equal(1, getResult.Affected);
        Assert.Equal("hello", getResult.Data![0]["name"]);
    }

    [Fact]
    public void PurgeDatabase_ThenSelectDatabase_Throws()
    {
        var db = _engine.GetOrCreateDatabase("testdb");
        db.QueryOne("purge database");

        // SelectDatabase should fail — DB no longer exists
        Assert.Throws<InvalidOperationException>(() =>
            _engine.SelectDatabase("testdb"));
    }
}

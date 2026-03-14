namespace SproutDB.Core.Tests;

public class MigrationTests : IDisposable
{
    private readonly string _tempDir;
    private readonly SproutEngine _engine;

    public MigrationTests()
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
    public void Migrate_RunsAllMigrations_InOrder()
    {
        var db = _engine.GetOrCreateDatabase("testdb");

        _engine.Migrate(typeof(TestMigrations.Schema.CreateUsers).Assembly, db);

        // CreateUsers (Order=1) creates users table, then AddEmail (Order=2) adds email column
        var desc = db.Query("describe users");
        Assert.Equal(SproutOperation.Describe, desc.Operation);
        Assert.NotNull(desc.Schema?.Columns);
        Assert.Contains(desc.Schema.Columns, c => c.Name == "name");
        Assert.Contains(desc.Schema.Columns, c => c.Name == "email");
    }

    [Fact]
    public void Migrate_SkipsAlreadyApplied()
    {
        var db = _engine.GetOrCreateDatabase("testdb");
        var assembly = typeof(TestMigrations.Schema.CreateUsers).Assembly;

        // Run twice
        _engine.Migrate(assembly, db);
        _engine.Migrate(assembly, db);

        // _migrations should have exactly 2 Once entries (CreateUsers + AddEmail)
        var r = db.Query("get _migrations");
        Assert.Equal(2, r.Affected);
    }

    [Fact]
    public void Migrate_ThrowsOnFailure_DoesNotWriteRecord()
    {
        var db = _engine.GetOrCreateDatabase("testdb");

        Assert.Throws<InvalidOperationException>(() =>
            _engine.Migrate(typeof(TestMigrations.Failing.FailingMigration).Assembly, db));

        // The failing migration should not be tracked
        var r = db.Query("get _migrations select name");
        if (r.Data is not null)
        {
            Assert.DoesNotContain(r.Data, row =>
            {
                var name = (string)row["name"];
                return name.Contains("FailingMigration");
            });
        }
    }

    [Fact]
    public void Migrate_OnStartup_RunsEveryTime()
    {
        var db = _engine.GetOrCreateDatabase("testdb");
        var assembly = typeof(TestMigrations.Startup.CreateStartupTable).Assembly;

        // First migrate: creates table + inserts startup marker
        _engine.Migrate(assembly, db);
        var r1 = db.Query("get startupcounter");
        Assert.Equal(1, r1.Affected);

        // Second migrate: OnStartup runs again, inserts another marker
        _engine.Migrate(assembly, db);
        var r2 = db.Query("get startupcounter");
        Assert.Equal(2, r2.Affected);
    }

    [Fact]
    public void Migrate_OnStartup_NotTracked()
    {
        var db = _engine.GetOrCreateDatabase("testdb");

        _engine.Migrate(typeof(TestMigrations.Startup.CreateStartupTable).Assembly, db);

        var r = db.Query("get _migrations");
        // Only the Once migration (CreateStartupTable) should be tracked
        Assert.Equal(1, r.Affected);
        Assert.Contains(r.Data, row =>
        {
            var name = (string)row["name"];
            return name.Contains("CreateStartupTable");
        });
        Assert.DoesNotContain(r.Data, row =>
        {
            var name = (string)row["name"];
            return name.Contains("StartupCleanup");
        });
    }

    [Fact]
    public void Migrate_Mixed_OrderPreserved()
    {
        var db = _engine.GetOrCreateDatabase("testdb");

        // StartupCleanup depends on CreateStartupTable — Order ensures correct execution
        _engine.Migrate(typeof(TestMigrations.Startup.CreateStartupTable).Assembly, db);

        // If order was wrong, StartupCleanup would fail because table doesn't exist yet
        var r = db.Query("get startupcounter");
        Assert.Equal(1, r.Affected);
    }

    [Fact]
    public void Migrate_ThrowsSproutMigrationException_OnQueryError()
    {
        var db = _engine.GetOrCreateDatabase("testdb");

        var ex = Assert.Throws<SproutMigrationException>(() =>
            _engine.Migrate(typeof(TestMigrations.SilentFailing.SilentlyFailingMigration).Assembly, db));

        Assert.Contains("SilentlyFailingMigration", ex.MigrationName);
        Assert.Equal("UNKNOWN_TABLE", ex.ErrorCode);
        Assert.Contains("nonexistent_table", ex.Query);
    }

    [Fact]
    public void Migrate_SilentFailure_DoesNotTrackMigration()
    {
        var db = _engine.GetOrCreateDatabase("testdb");

        Assert.Throws<SproutMigrationException>(() =>
            _engine.Migrate(typeof(TestMigrations.SilentFailing.SilentlyFailingMigration).Assembly, db));

        var r = db.Query("get _migrations select name");
        if (r.Data is not null)
        {
            Assert.DoesNotContain(r.Data, row =>
            {
                var name = (string)row["name"];
                return name.Contains("SilentlyFailingMigration");
            });
        }
    }

    [Fact]
    public void GetOrCreateDatabase_CreatesIfNotExists()
    {
        var db = _engine.GetOrCreateDatabase("newdb");

        Assert.Equal("newdb", db.Name);

        // Verify database exists by creating a table in it
        var r = db.Query("create table test (val sint)");
        Assert.Equal(SproutOperation.CreateTable, r.Operation);
    }

    [Fact]
    public void SelectDatabase_ThrowsIfNotExists()
    {
        Assert.Throws<InvalidOperationException>(() =>
            _engine.SelectDatabase("nonexistent"));
    }

    [Fact]
    public void GetDatabases_ReturnsAll()
    {
        _engine.GetOrCreateDatabase("db1");
        _engine.GetOrCreateDatabase("db2");
        _engine.GetOrCreateDatabase("db3");

        var databases = _engine.GetDatabases();

        // 3 user databases + _system
        Assert.Equal(4, databases.Count);
        Assert.Contains(databases, d => d.Name == "db1");
        Assert.Contains(databases, d => d.Name == "db2");
        Assert.Contains(databases, d => d.Name == "db3");
        Assert.Contains(databases, d => d.Name == "_system");
    }
}

namespace SproutDB.Core.Tests;

public class BackupRestoreTests : IDisposable
{
    private readonly string _tempDir;
    private readonly SproutEngine _engine;

    public BackupRestoreTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), $"sproutdb-test-{Guid.NewGuid()}");
        _engine = new SproutEngine(_tempDir);
        _engine.ExecuteOne("create database", "testdb");
        _engine.ExecuteOne("create table users (name string 100, age ubyte)", "testdb");
        _engine.ExecuteOne("upsert users {name: 'Alice', age: 25}", "testdb");
        _engine.ExecuteOne("upsert users {name: 'Bob', age: 30}", "testdb");
    }

    public void Dispose()
    {
        _engine.Dispose();
        if (Directory.Exists(_tempDir))
            Directory.Delete(_tempDir, true);
    }

    // ── Backup ────────────────────────────────────────────────

    [Fact]
    public void Backup_Success()
    {
        var r = _engine.ExecuteOne("backup", "testdb");

        Assert.Equal(SproutOperation.Backup, r.Operation);
        Assert.Null(r.Errors);
        Assert.NotNull(r.BackupPath);
        Assert.True(File.Exists(r.BackupPath));
        Assert.Equal("testdb", r.Schema?.Database);
    }

    [Fact]
    public void Backup_CreatesZipFile()
    {
        var r = _engine.ExecuteOne("backup", "testdb");

        Assert.NotNull(r.BackupPath);
        Assert.EndsWith(".zip", r.BackupPath);
        Assert.True(new FileInfo(r.BackupPath).Length > 0);
    }

    [Fact]
    public void Backup_ZipContainsTableFiles()
    {
        var r = _engine.ExecuteOne("backup", "testdb");

        using var zip = System.IO.Compression.ZipFile.OpenRead(r.BackupPath!);
        var entryNames = zip.Entries.Select(e => e.FullName).ToList();

        Assert.Contains("users/_schema.bin", entryNames);
        Assert.Contains("users/name.col", entryNames);
        Assert.Contains("users/age.col", entryNames);
        Assert.Contains("users/_index", entryNames);
    }

    [Fact]
    public void Backup_ExcludesWal()
    {
        var r = _engine.ExecuteOne("backup", "testdb");

        using var zip = System.IO.Compression.ZipFile.OpenRead(r.BackupPath!);
        var entryNames = zip.Entries.Select(e => e.FullName).ToList();

        Assert.DoesNotContain("_wal", entryNames);
    }

    [Fact]
    public void Backup_UnknownDatabase_Error()
    {
        var r = _engine.ExecuteOne("backup", "nope");

        Assert.Equal(SproutOperation.Error, r.Operation);
        Assert.Equal("UNKNOWN_DATABASE", r.Errors![0].Code);
    }

    // ── Restore ───────────────────────────────────────────────

    [Fact]
    public void Restore_RecoverData()
    {
        // Backup
        var backup = _engine.ExecuteOne("backup", "testdb");

        // Purge the database
        _engine.ExecuteOne("purge database", "testdb");
        var gone = _engine.ExecuteOne("get users", "testdb");
        Assert.Equal(SproutOperation.Error, gone.Operation);

        // Restore
        var r = _engine.ExecuteOne($"restore '{backup.BackupPath}'", "testdb");
        Assert.Equal(SproutOperation.Restore, r.Operation);
        Assert.Null(r.Errors);
        Assert.Equal("testdb", r.Schema?.Database);

        // Data is back
        var data = _engine.ExecuteOne("get users", "testdb");
        Assert.Equal(2, data.Affected);
        Assert.Equal("Alice", data.Data![0]["name"]);
        Assert.Equal("Bob", data.Data[1]["name"]);
    }

    [Fact]
    public void Restore_OverwritesExistingDatabase()
    {
        var backup = _engine.ExecuteOne("backup", "testdb");

        // Add more data
        _engine.ExecuteOne("upsert users {name: 'Charlie', age: 35}", "testdb");
        var before = _engine.ExecuteOne("get users", "testdb");
        Assert.Equal(3, before.Affected);

        // Restore from backup (should overwrite)
        _engine.ExecuteOne($"restore '{backup.BackupPath}'", "testdb");

        var after = _engine.ExecuteOne("get users", "testdb");
        Assert.Equal(2, after.Affected); // back to 2
    }

    [Fact]
    public void Restore_ToNewDatabase()
    {
        var backup = _engine.ExecuteOne("backup", "testdb");

        // Restore to a different database name
        var r = _engine.ExecuteOne($"restore '{backup.BackupPath}'", "newdb");
        Assert.Equal(SproutOperation.Restore, r.Operation);
        Assert.Null(r.Errors);

        var data = _engine.ExecuteOne("get users", "newdb");
        Assert.Equal(2, data.Affected);
    }

    [Fact]
    public void Restore_MissingFile_Error()
    {
        var r = _engine.ExecuteOne("restore '/nonexistent/backup.zip'", "testdb");

        Assert.Equal(SproutOperation.Error, r.Operation);
        Assert.Contains("does not exist", r.Errors![0].Message);
    }

    [Fact]
    public void Restore_SchemaPreserved()
    {
        var backup = _engine.ExecuteOne("backup", "testdb");
        _engine.ExecuteOne("purge database", "testdb");

        _engine.ExecuteOne($"restore '{backup.BackupPath}'", "testdb");

        var desc = _engine.ExecuteOne("describe users", "testdb");
        Assert.Equal(SproutOperation.Describe, desc.Operation);
        var cols = desc.Schema?.Columns;
        Assert.NotNull(cols);
        Assert.Contains(cols, c => c.Name == "name");
        Assert.Contains(cols, c => c.Name == "age");
    }

    // ── Backup after schema changes ───────────────────────────

    [Fact]
    public void Backup_AfterAddColumn_Preserved()
    {
        _engine.ExecuteOne("add column users.email string 320", "testdb");
        _engine.ExecuteOne("upsert users {_id: 1, email: 'alice@test.com'}", "testdb");

        var backup = _engine.ExecuteOne("backup", "testdb");
        _engine.ExecuteOne("purge database", "testdb");
        _engine.ExecuteOne($"restore '{backup.BackupPath}'", "testdb");

        var data = _engine.ExecuteOne("get users select name, email", "testdb");
        Assert.Equal("alice@test.com", data.Data![0]["email"]);
    }
}

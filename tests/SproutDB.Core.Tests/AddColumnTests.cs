namespace SproutDB.Core.Tests;

public class AddColumnTests : IDisposable
{
    private readonly string _tempDir;
    private readonly SproutEngine _engine;

    public AddColumnTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), $"sproutdb-test-{Guid.NewGuid()}");
        _engine = new SproutEngine(_tempDir);
        _engine.ExecuteOne("create database", "testdb");
        _engine.ExecuteOne("create table users (name string 100, age ubyte)", "testdb");
    }

    public void Dispose()
    {
        _engine.Dispose();
        if (Directory.Exists(_tempDir))
            Directory.Delete(_tempDir, true);
    }

    // ── New column ──────────────────────────────────────────

    [Fact]
    public void AddColumn_Success()
    {
        var r = _engine.ExecuteOne("add column users.email string 320", "testdb");

        Assert.Equal(SproutOperation.AddColumn, r.Operation);
        Assert.NotNull(r.Schema);
        Assert.Equal("users", r.Schema.Table);
        Assert.Null(r.Errors);
    }

    [Fact]
    public void AddColumn_ResponseIncludesAllColumns()
    {
        var r = _engine.ExecuteOne("add column users.email string 320", "testdb");

        var cols = r.Schema!.Columns!;
        Assert.Equal(4, cols.Count); // _id + name + age + email
        Assert.Equal("_id", cols[0].Name);
        Assert.Equal("name", cols[1].Name);
        Assert.Equal("age", cols[2].Name);
        Assert.Equal("email", cols[3].Name);
        Assert.Equal(320, cols[3].Size);
    }

    [Fact]
    public void AddColumn_CreatesColFile()
    {
        _engine.ExecuteOne("add column users.email string 320", "testdb");

        var colPath = Path.Combine(_tempDir, "testdb", "users", "email.col");
        Assert.True(File.Exists(colPath));
        Assert.Equal((long)10_000 * 321, new FileInfo(colPath).Length);
    }

    [Fact]
    public void AddColumn_UpdatesSchema()
    {
        _engine.ExecuteOne("add column users.email string 320 strict", "testdb");

        // Verify the response has the right columns (schema is binary now)
        var r = _engine.ExecuteOne("add column users.email string 320 strict", "testdb"); // idempotent re-read
        var cols = r.Schema!.Columns!;
        var email = cols.First(c => c.Name == "email");
        Assert.Equal("string", email.Type);
        Assert.Equal(320, email.Size);
        Assert.True(email.Strict);

        // Also verify binary file exists
        var schemaPath = Path.Combine(_tempDir, "testdb", "users", "_schema.bin");
        Assert.True(File.Exists(schemaPath));
    }

    [Fact]
    public void AddColumn_UpsertCanUseNewColumn()
    {
        _engine.ExecuteOne("add column users.email string 320", "testdb");
        var r = _engine.ExecuteOne("upsert users {name: 'John', email: 'john@test.com'}", "testdb");

        Assert.Equal(SproutOperation.Upsert, r.Operation);
        Assert.Equal("john@test.com", r.Data![0]["email"]);
    }

    // ── Default backfill ────────────────────────────────────

    [Fact]
    public void AddColumn_WithDefault_BackfillsExistingRows()
    {
        // Insert some rows first
        _engine.ExecuteOne("upsert users {name: 'John', age: 25}", "testdb");
        _engine.ExecuteOne("upsert users {name: 'Jane', age: 30}", "testdb");

        // Add column with default
        _engine.ExecuteOne("add column users.active bool default true", "testdb");

        // Update row 1 to read it back — active should be true (backfilled)
        var r = _engine.ExecuteOne("upsert users {_id: 1, name: 'John'}", "testdb");
        Assert.Equal(true, r.Data![0]["active"]);

        var r2 = _engine.ExecuteOne("upsert users {_id: 2, name: 'Jane'}", "testdb");
        Assert.Equal(true, r2.Data![0]["active"]);
    }

    [Fact]
    public void AddColumn_NullableNoDefault_ExistingRowsAreNull()
    {
        _engine.ExecuteOne("upsert users {name: 'John'}", "testdb");
        _engine.ExecuteOne("add column users.email string 320", "testdb");

        // Read back via update
        var r = _engine.ExecuteOne("upsert users {_id: 1, name: 'John'}", "testdb");
        Assert.Null(r.Data![0]["email"]);
    }

    // ── Idempotent (same type) ──────────────────────────────

    [Fact]
    public void AddColumn_SameType_SilentOk()
    {
        _engine.ExecuteOne("add column users.email string 320", "testdb");
        var r = _engine.ExecuteOne("add column users.email string 320", "testdb");

        Assert.Equal(SproutOperation.AddColumn, r.Operation);
        Assert.Null(r.Errors);
    }

    // ── Type expansion ──────────────────────────────────────

    [Fact]
    public void AddColumn_TypeExpansion_UbyteToUshort()
    {
        // age is ubyte
        var r = _engine.ExecuteOne("add column users.age ushort", "testdb");

        Assert.Equal(SproutOperation.AddColumn, r.Operation);
        Assert.Null(r.Errors);

        // Schema should now say ushort
        var ageCol = r.Schema!.Columns!.First(c => c.Name == "age");
        Assert.Equal("ushort", ageCol.Type);
    }

    [Fact]
    public void AddColumn_TypeExpansion_FloatToDouble()
    {
        _engine.ExecuteOne("create table metrics (value float)", "testdb");
        var r = _engine.ExecuteOne("add column metrics.value double", "testdb");

        Assert.Equal(SproutOperation.AddColumn, r.Operation);
        var col = r.Schema!.Columns!.First(c => c.Name == "value");
        Assert.Equal("double", col.Type);
    }

    // ── Type narrowing → error ──────────────────────────────

    [Fact]
    public void AddColumn_TypeNarrowing_Error()
    {
        // age is ubyte, try to narrow to... wait, let's widen first then narrow
        _engine.ExecuteOne("add column users.age ushort", "testdb"); // expand to ushort
        var r = _engine.ExecuteOne("add column users.age ubyte", "testdb"); // try to narrow back

        Assert.Equal(SproutOperation.Error, r.Operation);
        Assert.Equal("TYPE_NARROWING", r.Errors![0].Code);
    }

    [Fact]
    public void AddColumn_IncompatibleType_Error()
    {
        // age is ubyte, try bool
        var r = _engine.ExecuteOne("add column users.age bool", "testdb");

        Assert.Equal(SproutOperation.Error, r.Operation);
        Assert.Equal("TYPE_NARROWING", r.Errors![0].Code);
    }

    // ── Strict violation ────────────────────────────────────

    [Fact]
    public void AddColumn_StrictViolation_Error()
    {
        _engine.ExecuteOne("create table t (score ubyte strict)", "testdb");
        var r = _engine.ExecuteOne("add column t.score ushort", "testdb");

        Assert.Equal(SproutOperation.Error, r.Operation);
        Assert.Equal("STRICT_VIOLATION", r.Errors![0].Code);
        Assert.Contains("strict", r.Errors[0].Message);
    }

    // ── Error cases ─────────────────────────────────────────

    [Fact]
    public void AddColumn_UnknownTable_Error()
    {
        var r = _engine.ExecuteOne("add column missing.col string", "testdb");

        Assert.Equal(SproutOperation.Error, r.Operation);
        Assert.Equal("UNKNOWN_TABLE", r.Errors![0].Code);
    }

    [Fact]
    public void AddColumn_UnknownDatabase_Error()
    {
        var r = _engine.ExecuteOne("add column users.col string", "nope");

        Assert.Equal(SproutOperation.Error, r.Operation);
        Assert.Equal("UNKNOWN_DATABASE", r.Errors![0].Code);
    }
}

namespace SproutDB.Core.Tests;

public class UpsertTests : IDisposable
{
    private readonly string _tempDir;
    private readonly SproutEngine _engine;

    public UpsertTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), $"sproutdb-test-{Guid.NewGuid()}");
        _engine = new SproutEngine(_tempDir);
        _engine.Execute("create database", "testdb");
        _engine.Execute(
            "create table users (name string 100, email string 320 strict, age ubyte, active bool default true, score sint)",
            "testdb");
    }

    public void Dispose()
    {
        _engine.Dispose();
        if (Directory.Exists(_tempDir))
            Directory.Delete(_tempDir, true);
    }

    // ── Insert ──────────────────────────────────────────────

    [Fact]
    public void Insert_ReturnsId1()
    {
        var r = _engine.Execute("upsert users {name: 'John'}", "testdb");

        Assert.Equal(SproutOperation.Upsert, r.Operation);
        Assert.Equal(1, r.Affected);
        Assert.NotNull(r.Data);
        Assert.Single(r.Data);
        Assert.Equal((ulong)1, r.Data[0]["_id"]);
    }

    [Fact]
    public void Insert_AutoIncrementsId()
    {
        _engine.Execute("upsert users {name: 'John'}", "testdb");
        var r2 = _engine.Execute("upsert users {name: 'Jane'}", "testdb");

        Assert.Equal((ulong)2, r2.Data![0]["_id"]);
    }

    [Fact]
    public void Insert_ReturnsFullRecord()
    {
        var r = _engine.Execute("upsert users {name: 'John', email: 'john@test.com', age: 25}", "testdb");

        var row = r.Data![0];
        Assert.Equal((ulong)1, row["_id"]);
        Assert.Equal("John", row["name"]);
        Assert.Equal("john@test.com", row["email"]);
        Assert.Equal((byte)25, row["age"]);
        Assert.Equal(true, row["active"]); // default
        Assert.Null(row["score"]);          // nullable, no value
    }

    [Fact]
    public void Insert_EmptyObject_OnlyDefaults()
    {
        var r = _engine.Execute("upsert users {}", "testdb");

        var row = r.Data![0];
        Assert.Equal((ulong)1, row["_id"]);
        Assert.Null(row["name"]);
        Assert.Null(row["email"]);
        Assert.Null(row["age"]);
        Assert.Equal(true, row["active"]); // default
        Assert.Null(row["score"]);
    }

    [Fact]
    public void Insert_NullOnNullableColumn()
    {
        var r = _engine.Execute("upsert users {name: null}", "testdb");

        Assert.Equal(SproutOperation.Upsert, r.Operation);
        Assert.Null(r.Data![0]["name"]);
    }

    // ── Update ──────────────────────────────────────────────

    [Fact]
    public void Update_WithExplicitId()
    {
        _engine.Execute("upsert users {name: 'John', age: 25}", "testdb");
        var r = _engine.Execute("upsert users {_id: 1, name: 'John Doe'}", "testdb");

        Assert.Equal(SproutOperation.Upsert, r.Operation);
        Assert.Equal(1, r.Affected);

        var row = r.Data![0];
        Assert.Equal((ulong)1, row["_id"]);
        Assert.Equal("John Doe", row["name"]);
        Assert.Equal((byte)25, row["age"]);  // unchanged
    }

    [Fact]
    public void Update_SetToNull()
    {
        _engine.Execute("upsert users {name: 'John', age: 25}", "testdb");
        var r = _engine.Execute("upsert users {_id: 1, age: null}", "testdb");

        Assert.Null(r.Data![0]["age"]);
        Assert.Equal("John", r.Data[0]["name"]); // unchanged
    }

    [Fact]
    public void Update_PreservesUnchangedFields()
    {
        _engine.Execute("upsert users {name: 'John', email: 'john@test.com', age: 25, score: -100}", "testdb");
        var r = _engine.Execute("upsert users {_id: 1, email: 'new@test.com'}", "testdb");

        var row = r.Data![0];
        Assert.Equal("John", row["name"]);
        Assert.Equal("new@test.com", row["email"]);
        Assert.Equal((byte)25, row["age"]);
        Assert.Equal(true, row["active"]);
        Assert.Equal(-100, row["score"]);
    }

    [Fact]
    public void Upsert_NewId_NonExistent_ReturnsError()
    {
        var r = _engine.Execute("upsert users {_id: 42, name: 'John'}", "testdb");

        Assert.Equal(SproutOperation.Error, r.Operation);
        Assert.Equal("ID_NOT_FOUND", r.Errors?[0].Code);
    }

    [Fact]
    public void Upsert_ExplicitId_UpdatesExistingRow()
    {
        var insert = _engine.Execute("upsert users {name: 'John'}", "testdb");
        var id = insert.Data?[0]["_id"];
        Assert.NotNull(id);

        var r = _engine.Execute($"upsert users {{_id: {id}, name: 'Jane'}}", "testdb");

        Assert.Equal(SproutOperation.Upsert, r.Operation);
        Assert.Equal("Jane", r.Data?[0]["name"]);
    }

    // ── Type handling ───────────────────────────────────────

    [Fact]
    public void Insert_AllNumericTypes()
    {
        _engine.Execute(
            "create table nums (a sbyte, b ubyte, c sshort, d ushort, e sint, f uint, g slong, h ulong, i float, j double)",
            "testdb");

        var r = _engine.Execute(
            "upsert nums {a: -1, b: 255, c: -1000, d: 60000, e: -100000, f: 100000, g: -999999999, h: 999999999, i: 3.14, j: 2.71828}",
            "testdb");

        var row = r.Data![0];
        Assert.Equal((sbyte)-1, row["a"]);
        Assert.Equal((byte)255, row["b"]);
        Assert.Equal((short)-1000, row["c"]);
        Assert.Equal((ushort)60000, row["d"]);
        Assert.Equal(-100000, row["e"]);
        Assert.Equal(100000u, row["f"]);
        Assert.Equal(-999999999L, row["g"]);
        Assert.Equal(999999999UL, row["h"]);
        Assert.IsType<float>(row["i"]);
        Assert.IsType<double>(row["j"]);
    }

    [Fact]
    public void Insert_BoolColumn()
    {
        var r = _engine.Execute("upsert users {active: false}", "testdb");
        Assert.Equal(false, r.Data![0]["active"]);
    }

    [Fact]
    public void Insert_FloatAcceptsInteger()
    {
        _engine.Execute("create table t (val double)", "testdb");
        var r = _engine.Execute("upsert t {val: 42}", "testdb");

        Assert.Equal(42.0, r.Data![0]["val"]);
    }

    // ── Error cases ─────────────────────────────────────────

    [Fact]
    public void UnknownTable_Error()
    {
        var r = _engine.Execute("upsert missing {name: 'x'}", "testdb");

        Assert.Equal(SproutOperation.Error, r.Operation);
        Assert.Equal("UNKNOWN_TABLE", r.Errors![0].Code);
    }

    [Fact]
    public void UnknownColumn_Error()
    {
        var r = _engine.Execute("upsert users {nonexistent: 'x'}", "testdb");

        Assert.Equal(SproutOperation.Error, r.Operation);
        Assert.Equal("UNKNOWN_COLUMN", r.Errors![0].Code);
        Assert.Contains("nonexistent", r.Errors[0].Message);
    }

    [Fact]
    public void TypeMismatch_StringToInt_Error()
    {
        var r = _engine.Execute("upsert users {age: 'twenty'}", "testdb");

        Assert.Equal(SproutOperation.Error, r.Operation);
        Assert.Equal("TYPE_MISMATCH", r.Errors![0].Code);
    }

    [Fact]
    public void TypeMismatch_IntToBool_Error()
    {
        var r = _engine.Execute("upsert users {active: 1}", "testdb");

        Assert.Equal(SproutOperation.Error, r.Operation);
        Assert.Equal("TYPE_MISMATCH", r.Errors![0].Code);
    }

    [Fact]
    public void NotNullable_Error()
    {
        var r = _engine.Execute("upsert users {active: null}", "testdb");

        Assert.Equal(SproutOperation.Error, r.Operation);
        Assert.Equal("NOT_NULLABLE", r.Errors![0].Code);
        Assert.Contains("active", r.Errors[0].Message);
        Assert.Contains("true", r.Errors[0].Message);
    }

    [Fact]
    public void UnknownDatabase_Error()
    {
        var r = _engine.Execute("upsert users {name: 'x'}", "missing");

        Assert.Equal(SproutOperation.Error, r.Operation);
        Assert.Equal("UNKNOWN_DATABASE", r.Errors![0].Code);
    }

    // ── ON clause ────────────────────────────────────────────

    [Fact]
    public void UpsertOn_NoMatch_Inserts()
    {
        var r = _engine.Execute("upsert users {email: 'john@test.com', name: 'John'} on email", "testdb");

        Assert.Equal(SproutOperation.Upsert, r.Operation);
        Assert.Equal(1, r.Affected);
        Assert.Equal((ulong)1, r.Data![0]["_id"]);
        Assert.Equal("john@test.com", r.Data[0]["email"]);
        Assert.Equal("John", r.Data[0]["name"]);
    }

    [Fact]
    public void UpsertOn_Match_Updates()
    {
        _engine.Execute("upsert users {email: 'john@test.com', name: 'John', age: 25}", "testdb");
        var r = _engine.Execute("upsert users {email: 'john@test.com', name: 'John Doe'} on email", "testdb");

        Assert.Equal(SproutOperation.Upsert, r.Operation);
        Assert.Equal(1, r.Affected);
        Assert.Equal((ulong)1, r.Data![0]["_id"]); // same record
        Assert.Equal("John Doe", r.Data[0]["name"]);
        Assert.Equal("john@test.com", r.Data[0]["email"]);
        Assert.Equal((byte)25, r.Data[0]["age"]); // unchanged
    }

    [Fact]
    public void UpsertOn_Match_PreservesUnchangedFields()
    {
        _engine.Execute("upsert users {email: 'john@test.com', name: 'John', age: 25, score: -50}", "testdb");
        var r = _engine.Execute("upsert users {email: 'john@test.com', age: 30} on email", "testdb");

        var row = r.Data![0];
        Assert.Equal((ulong)1, row["_id"]);
        Assert.Equal("John", row["name"]); // unchanged
        Assert.Equal((byte)30, row["age"]); // updated
        Assert.Equal(-50, row["score"]); // unchanged
    }

    [Fact]
    public void UpsertOn_SecondInsert_GetsNewId()
    {
        _engine.Execute("upsert users {email: 'john@test.com', name: 'John'} on email", "testdb");
        var r = _engine.Execute("upsert users {email: 'jane@test.com', name: 'Jane'} on email", "testdb");

        Assert.Equal((ulong)2, r.Data![0]["_id"]); // new record
        Assert.Equal("jane@test.com", r.Data[0]["email"]);
    }

    [Fact]
    public void UpsertOn_NumericMatch()
    {
        _engine.Execute("upsert users {age: 25, name: 'John'}", "testdb");
        var r = _engine.Execute("upsert users {age: 25, name: 'John Updated'} on age", "testdb");

        Assert.Equal((ulong)1, r.Data![0]["_id"]); // same record
        Assert.Equal("John Updated", r.Data[0]["name"]);
    }

    [Fact]
    public void UpsertOn_UnknownColumn_Error()
    {
        var r = _engine.Execute("upsert users {name: 'John'} on nonexistent", "testdb");

        Assert.Equal(SproutOperation.Error, r.Operation);
        Assert.Equal("UNKNOWN_COLUMN", r.Errors![0].Code);
    }

    [Fact]
    public void UpsertOn_ColumnNotInFields_Error()
    {
        var r = _engine.Execute("upsert users {name: 'John'} on email", "testdb");

        Assert.Equal(SproutOperation.Error, r.Operation);
        Assert.Equal("SYNTAX_ERROR", r.Errors![0].Code);
        Assert.Contains("email", r.Errors[0].Message);
    }

    [Fact]
    public void UpsertOn_WithExplicitId_Error()
    {
        var r = _engine.Execute("upsert users {_id: 1, email: 'john@test.com'} on email", "testdb");

        Assert.Equal(SproutOperation.Error, r.Operation);
        // Explicit _id check runs first — ID 1 doesn't exist
        Assert.Equal("ID_NOT_FOUND", r.Errors?[0].Code);
    }

    // ── Bulk upsert ─────────────────────────────────────────

    [Fact]
    public void Bulk_InsertsMultipleRecords()
    {
        var r = _engine.Execute("upsert users [{name: 'John', age: 25}, {name: 'Jane', age: 30}]", "testdb");

        Assert.Equal(SproutOperation.Upsert, r.Operation);
        Assert.Equal(2, r.Affected);
        Assert.Equal(2, r.Data!.Count);
        Assert.Equal((ulong)1, r.Data[0]["_id"]);
        Assert.Equal("John", r.Data[0]["name"]);
        Assert.Equal((ulong)2, r.Data[1]["_id"]);
        Assert.Equal("Jane", r.Data[1]["name"]);
    }

    [Fact]
    public void Bulk_WithOn_UpdatesAndInserts()
    {
        _engine.Execute("upsert users {email: 'john@test.com', name: 'John', age: 25}", "testdb");

        var r = _engine.Execute(
            "upsert users [{email: 'john@test.com', name: 'John Updated'}, {email: 'jane@test.com', name: 'Jane'}] on email",
            "testdb");

        Assert.Equal(2, r.Affected);
        Assert.Equal((ulong)1, r.Data![0]["_id"]); // updated existing
        Assert.Equal("John Updated", r.Data[0]["name"]);
        Assert.Equal((byte)25, r.Data[0]["age"]); // unchanged
        Assert.Equal((ulong)2, r.Data[1]["_id"]); // inserted new
        Assert.Equal("Jane", r.Data[1]["name"]);
    }

    [Fact]
    public void Bulk_WithOn_SinglePassMatchesAll()
    {
        _engine.Execute("upsert users {email: 'a@test.com', name: 'A'}", "testdb");
        _engine.Execute("upsert users {email: 'b@test.com', name: 'B'}", "testdb");

        var r = _engine.Execute(
            "upsert users [{email: 'b@test.com', name: 'B Updated'}, {email: 'a@test.com', name: 'A Updated'}] on email",
            "testdb");

        Assert.Equal(2, r.Affected);
        Assert.Equal((ulong)2, r.Data![0]["_id"]); // b@test.com = id 2
        Assert.Equal("B Updated", r.Data[0]["name"]);
        Assert.Equal((ulong)1, r.Data![1]["_id"]); // a@test.com = id 1
        Assert.Equal("A Updated", r.Data[1]["name"]);
    }

    [Fact]
    public void Bulk_ReturnsAllRecordsWithAllFields()
    {
        var r = _engine.Execute("upsert users [{name: 'John'}, {name: 'Jane'}]", "testdb");

        foreach (var row in r.Data!)
        {
            Assert.True(row.ContainsKey("_id"));
            Assert.True(row.ContainsKey("name"));
            Assert.True(row.ContainsKey("email"));
            Assert.True(row.ContainsKey("age"));
            Assert.True(row.ContainsKey("active"));
            Assert.True(row.ContainsKey("score"));
        }
    }

    [Fact]
    public void BulkLimit_Exceeded_Error()
    {
        // Default bulk limit is 100; create engine with limit of 2
        using var tempDir = new TempDir();
        using var engine = new SproutEngine(new SproutEngineSettings
        {
            DataDirectory = tempDir.Path,
            BulkLimit = 2,
        });
        engine.Execute("create database", "testdb");
        engine.Execute("create table users (name string 100)", "testdb");

        var r = engine.Execute("upsert users [{name: 'A'}, {name: 'B'}, {name: 'C'}]", "testdb");

        Assert.Equal(SproutOperation.Error, r.Operation);
        Assert.Equal("BULK_LIMIT", r.Errors![0].Code);
        Assert.Contains("3", r.Errors[0].Message);
    }

    [Fact]
    public void BulkLimit_AtLimit_Succeeds()
    {
        using var tempDir = new TempDir();
        using var engine = new SproutEngine(new SproutEngineSettings
        {
            DataDirectory = tempDir.Path,
            BulkLimit = 2,
        });
        engine.Execute("create database", "testdb");
        engine.Execute("create table users (name string 100)", "testdb");

        var r = engine.Execute("upsert users [{name: 'A'}, {name: 'B'}]", "testdb");

        Assert.Equal(SproutOperation.Upsert, r.Operation);
        Assert.Equal(2, r.Affected);
    }

    // ── Multi-error collection (#011) ──────────────────────

    /// <summary>
    /// Two unknown columns in a single upsert — both errors are collected.
    /// </summary>
    [Fact]
    public void MultipleUnknownColumns_AllReported()
    {
        var r = _engine.Execute("upsert users {foo: 'x', bar: 42}", "testdb");

        Assert.Equal(SproutOperation.Error, r.Operation);
        Assert.Equal(2, r.Errors!.Count);
        Assert.Equal("UNKNOWN_COLUMN", r.Errors[0].Code);
        Assert.Contains("foo", r.Errors[0].Message);
        Assert.Equal("UNKNOWN_COLUMN", r.Errors[1].Code);
        Assert.Contains("bar", r.Errors[1].Message);
    }

    /// <summary>
    /// Mix of unknown column and type mismatch — both collected.
    /// Known columns are type-checked even when unknown columns exist.
    /// </summary>
    [Fact]
    public void UnknownColumn_And_TypeMismatch_BothReported()
    {
        var r = _engine.Execute("upsert users {foo: 'x', age: 'twenty'}", "testdb");

        Assert.Equal(SproutOperation.Error, r.Operation);
        Assert.Equal(2, r.Errors!.Count);
        Assert.Equal("UNKNOWN_COLUMN", r.Errors[0].Code);
        Assert.Contains("foo", r.Errors[0].Message);
        Assert.Equal("TYPE_MISMATCH", r.Errors[1].Code);
        Assert.Contains("age", r.Errors[1].Message);
    }

    /// <summary>
    /// Multiple type mismatches in a single upsert — all collected.
    /// </summary>
    [Fact]
    public void MultipleTypeMismatches_AllReported()
    {
        var r = _engine.Execute("upsert users {age: 'twenty', active: 1}", "testdb");

        Assert.Equal(SproutOperation.Error, r.Operation);
        Assert.Equal(2, r.Errors!.Count);
        Assert.All(r.Errors, e => Assert.Equal("TYPE_MISMATCH", e.Code));
    }

    /// <summary>
    /// Invalid id plus unknown column — both collected.
    /// </summary>
    [Fact]
    public void InvalidId_And_UnknownColumn_BothReported()
    {
        var r = _engine.Execute("upsert users {_id: -5, foo: 'x'}", "testdb");

        Assert.Equal(SproutOperation.Error, r.Operation);
        Assert.Equal(2, r.Errors!.Count);
        Assert.Equal("TYPE_MISMATCH", r.Errors[0].Code);
        Assert.Contains("_id", r.Errors[0].Message);
        Assert.Equal("UNKNOWN_COLUMN", r.Errors[1].Code);
        Assert.Contains("foo", r.Errors[1].Message);
    }

    /// <summary>
    /// AnnotatedQuery places error comments inline at the exact field positions.
    /// </summary>
    [Fact]
    public void MultipleErrors_AnnotatedQueryInline()
    {
        var r = _engine.Execute("upsert users {foo: 'x', bar: 42}", "testdb");

        Assert.NotNull(r.AnnotatedQuery);
        // Errors are annotated inline after the field name
        Assert.Equal(
            "upsert users {foo ##column 'foo' does not exist##: 'x', bar ##column 'bar' does not exist##: 42}",
            r.AnnotatedQuery);
    }

    /// <summary>
    /// Single error still works correctly with inline annotation.
    /// </summary>
    [Fact]
    public void SingleError_InlineAnnotation()
    {
        var r = _engine.Execute("upsert users {nonexistent: 'x'}", "testdb");

        Assert.Equal(SproutOperation.Error, r.Operation);
        Assert.Single(r.Errors!);
        Assert.Equal("UNKNOWN_COLUMN", r.Errors[0].Code);
        Assert.Equal(
            "upsert users {nonexistent ##column 'nonexistent' does not exist##: 'x'}",
            r.AnnotatedQuery);
    }

    // Helper for tests that need custom settings
    private sealed class TempDir : IDisposable
    {
        public string Path { get; } = System.IO.Path.Combine(
            System.IO.Path.GetTempPath(), $"sproutdb-test-{Guid.NewGuid()}");
        public void Dispose()
        {
            if (Directory.Exists(Path))
                Directory.Delete(Path, true);
        }
    }
}

namespace SproutDB.Core.Tests;

public class IndexTests : IDisposable
{
    private readonly string _tempDir;
    private SproutEngine _engine;
    private bool _disposed;

    public IndexTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), $"sproutdb-test-{Guid.NewGuid()}");
        _engine = new SproutEngine(_tempDir);
        _engine.ExecuteOne("create database", "testdb");
        _engine.ExecuteOne(
            "create table users (name string 100, email string 200, age ubyte)",
            "testdb");
    }

    public void Dispose()
    {
        if (!_disposed)
        {
            _disposed = true;
            _engine.Dispose();
        }
        if (Directory.Exists(_tempDir))
            Directory.Delete(_tempDir, true);
    }

    // ── Create / Purge Index ──────────────────────────────

    [Fact]
    public void CreateIndex_BuildsCorrectly()
    {
        SeedUsers(5);

        var result = _engine.ExecuteOne("create index users.email", "testdb");

        Assert.Equal(SproutOperation.CreateIndex, result.Operation);
        Assert.Equal(1, result.Affected);

        // .btree file should exist
        var btreePath = Path.Combine(_tempDir, "testdb", "users", "email.btree");
        Assert.True(File.Exists(btreePath));
    }

    [Fact]
    public void CreateIndex_AlreadyExists_Error()
    {
        SeedUsers(1);
        _engine.ExecuteOne("create index users.email", "testdb");

        var result = _engine.ExecuteOne("create index users.email", "testdb");

        Assert.NotNull(result.Errors);
        Assert.Contains("already exists", result.Errors[0].Message);
    }

    [Fact]
    public void CreateIndex_UnknownColumn_Error()
    {
        var result = _engine.ExecuteOne("create index users.nonexistent", "testdb");

        Assert.NotNull(result.Errors);
        Assert.Contains("does not exist", result.Errors[0].Message);
    }

    [Fact]
    public void PurgeIndex_RemovesFile()
    {
        SeedUsers(3);
        _engine.ExecuteOne("create index users.email", "testdb");

        var result = _engine.ExecuteOne("purge index users.email", "testdb");

        Assert.Equal(SproutOperation.PurgeIndex, result.Operation);
        Assert.Equal(1, result.Affected);

        var btreePath = Path.Combine(_tempDir, "testdb", "users", "email.btree");
        Assert.False(File.Exists(btreePath));
    }

    [Fact]
    public void PurgeIndex_NotFound_Error()
    {
        var result = _engine.ExecuteOne("purge index users.email", "testdb");

        Assert.NotNull(result.Errors);
        Assert.Contains("does not exist", result.Errors[0].Message);
    }

    // ── GET with Index ─────────────────────────────────────

    [Fact]
    public void GetWithIndex_EqualityLookup_CorrectResult()
    {
        for (int i = 0; i < 100; i++)
            _engine.ExecuteOne($"upsert users {{name: 'User{i}', email: 'user{i}@test.com', age: {(i % 50) + 18}}}", "testdb");

        _engine.ExecuteOne("create index users.email", "testdb");

        var result = _engine.ExecuteOne("get users where email = 'user42@test.com'", "testdb");

        Assert.Null(result.Errors);
        Assert.NotNull(result.Data);
        Assert.Single(result.Data);
        Assert.Equal("User42", result.Data[0]["name"]);
    }

    [Fact]
    public void GetWithIndex_RangeLookup_CorrectResult()
    {
        for (int i = 0; i < 20; i++)
            _engine.ExecuteOne($"upsert users {{name: 'User{i}', email: 'user{i}@test.com', age: {i + 18}}}", "testdb");

        _engine.ExecuteOne("create index users.age", "testdb");

        // age > 30 means age 31..37 (i=13..19)
        var result = _engine.ExecuteOne("get users where age > 30", "testdb");

        Assert.Null(result.Errors);
        Assert.NotNull(result.Data);
        Assert.Equal(7, result.Data.Count);
        foreach (var row in result.Data)
        {
            var age = Convert.ToInt32(row["age"]);
            Assert.True(age > 30);
        }
    }

    [Fact]
    public void GetWithIndex_BetweenLookup_CorrectResult()
    {
        for (int i = 0; i < 20; i++)
            _engine.ExecuteOne($"upsert users {{name: 'User{i}', email: 'user{i}@test.com', age: {i + 18}}}", "testdb");

        _engine.ExecuteOne("create index users.age", "testdb");

        // age between 25 and 30 → i=7..12 → 6 rows
        var result = _engine.ExecuteOne("get users where age between 25 and 30", "testdb");

        Assert.Null(result.Errors);
        Assert.NotNull(result.Data);
        Assert.Equal(6, result.Data.Count);
        foreach (var row in result.Data)
        {
            var age = Convert.ToInt32(row["age"]);
            Assert.True(age >= 25 && age <= 30);
        }
    }

    // ── Upsert updates index ──────────────────────────────

    [Fact]
    public void UpsertUpdatesIndex()
    {
        _engine.ExecuteOne("upsert users {name: 'Alice', email: 'alice@test.com', age: 28}", "testdb");
        _engine.ExecuteOne("create index users.email", "testdb");

        // Update email
        _engine.ExecuteOne("upsert users {_id: 1, email: 'newalice@test.com'}", "testdb");

        // Old email should not be found
        var oldResult = _engine.ExecuteOne("get users where email = 'alice@test.com'", "testdb");
        Assert.NotNull(oldResult.Data);
        Assert.Empty(oldResult.Data);

        // New email should be found
        var newResult = _engine.ExecuteOne("get users where email = 'newalice@test.com'", "testdb");
        Assert.NotNull(newResult.Data);
        Assert.Single(newResult.Data);
        Assert.Equal("Alice", newResult.Data[0]["name"]);
    }

    // ── Delete updates index ──────────────────────────────

    [Fact]
    public void DeleteUpdatesIndex()
    {
        _engine.ExecuteOne("upsert users {name: 'Alice', email: 'alice@test.com', age: 28}", "testdb");
        _engine.ExecuteOne("upsert users {name: 'Bob', email: 'bob@test.com', age: 35}", "testdb");
        _engine.ExecuteOne("create index users.email", "testdb");

        _engine.ExecuteOne("delete users where name = 'Alice'", "testdb");

        // Deleted email should not be found
        var result = _engine.ExecuteOne("get users where email = 'alice@test.com'", "testdb");
        Assert.NotNull(result.Data);
        Assert.Empty(result.Data);

        // Bob's email should still be found
        var bobResult = _engine.ExecuteOne("get users where email = 'bob@test.com'", "testdb");
        Assert.NotNull(bobResult.Data);
        Assert.Single(bobResult.Data);
    }

    // ── B-Tree persist and reload ─────────────────────────

    [Fact]
    public void BTreePersistAndReload()
    {
        SeedUsers(10);
        _engine.ExecuteOne("create index users.email", "testdb");

        // Verify lookup works before dispose
        var before = _engine.ExecuteOne("get users where email = 'user3@test.com'", "testdb");
        Assert.NotNull(before.Data);
        Assert.Single(before.Data);

        // Dispose and reopen
        _engine.Dispose();
        _disposed = true;

        // Reopen engine — B-Tree should be reloaded from disk
        _engine = new SproutEngine(_tempDir);
        _disposed = false;

        var after = _engine.ExecuteOne("get users where email = 'user3@test.com'", "testdb");
        Assert.NotNull(after.Data);
        Assert.Single(after.Data);
        Assert.Equal("User3", after.Data[0]["name"]);
    }

    // ── GetWithoutIndex still works ────────────────────────

    [Fact]
    public void GetWithoutIndex_StillScansCorrectly()
    {
        SeedUsers(5);

        // No index — should still work via full scan
        var result = _engine.ExecuteOne("get users where email = 'user2@test.com'", "testdb");

        Assert.Null(result.Errors);
        Assert.NotNull(result.Data);
        Assert.Single(result.Data);
        Assert.Equal("User2", result.Data[0]["name"]);
    }

    // ── Unique Constraint ────────────────────────────────

    [Fact]
    public void UniqueIndex_CreateSucceeds_WhenNoDuplicates()
    {
        _engine.ExecuteOne("upsert users {name: 'Alice', email: 'alice@test.com', age: 28}", "testdb");
        _engine.ExecuteOne("upsert users {name: 'Bob', email: 'bob@test.com', age: 35}", "testdb");

        var result = _engine.ExecuteOne("create index unique users.email", "testdb");

        Assert.Equal(SproutOperation.CreateIndex, result.Operation);
        Assert.Null(result.Errors);
    }

    [Fact]
    public void UniqueIndex_CreateFails_WhenDuplicatesExist()
    {
        _engine.ExecuteOne("upsert users {name: 'Alice', email: 'same@test.com', age: 28}", "testdb");
        _engine.ExecuteOne("upsert users {name: 'Bob', email: 'same@test.com', age: 35}", "testdb");

        var result = _engine.ExecuteOne("create index unique users.email", "testdb");

        Assert.NotNull(result.Errors);
        Assert.Contains("duplicate values", result.Errors[0].Message);
    }

    [Fact]
    public void UniqueIndex_InsertDuplicate_Error()
    {
        _engine.ExecuteOne("upsert users {name: 'Alice', email: 'alice@test.com', age: 28}", "testdb");
        _engine.ExecuteOne("create index unique users.email", "testdb");

        var result = _engine.ExecuteOne("upsert users {name: 'Bob', email: 'alice@test.com', age: 35}", "testdb");

        Assert.NotNull(result.Errors);
        Assert.Equal("UNIQUE_VIOLATION", result.Errors[0].Code);
        Assert.Contains("alice@test.com", result.Errors[0].Message);
    }

    [Fact]
    public void UniqueIndex_InsertUniqueValue_Succeeds()
    {
        _engine.ExecuteOne("upsert users {name: 'Alice', email: 'alice@test.com', age: 28}", "testdb");
        _engine.ExecuteOne("create index unique users.email", "testdb");

        var result = _engine.ExecuteOne("upsert users {name: 'Bob', email: 'bob@test.com', age: 35}", "testdb");

        Assert.Null(result.Errors);
        Assert.Equal(SproutOperation.Upsert, result.Operation);
    }

    [Fact]
    public void UniqueIndex_UpdateSameRow_Allowed()
    {
        _engine.ExecuteOne("upsert users {name: 'Alice', email: 'alice@test.com', age: 28}", "testdb");
        _engine.ExecuteOne("create index unique users.email", "testdb");

        // Update the same row — same email value, should be allowed
        var result = _engine.ExecuteOne("upsert users {_id: 1, name: 'Alice Updated', email: 'alice@test.com', age: 29}", "testdb");

        Assert.Null(result.Errors);
        Assert.Equal(SproutOperation.Upsert, result.Operation);
    }

    [Fact]
    public void UniqueIndex_NullValues_AllowedMultiple()
    {
        // name column has no default → nullable
        _engine.ExecuteOne("create index unique users.name", "testdb");

        var r1 = _engine.ExecuteOne("upsert users {email: 'a@test.com', age: 20}", "testdb");
        Assert.Null(r1.Errors);

        var r2 = _engine.ExecuteOne("upsert users {email: 'b@test.com', age: 25}", "testdb");
        Assert.Null(r2.Errors);
    }

    [Fact]
    public void UniqueIndex_BatchDuplicate_Error()
    {
        _engine.ExecuteOne("create index unique users.email", "testdb");

        var result = _engine.ExecuteOne(
            "upsert users [{name: 'Alice', email: 'same@test.com', age: 28}, {name: 'Bob', email: 'same@test.com', age: 35}]",
            "testdb");

        Assert.NotNull(result.Errors);
        Assert.Equal("UNIQUE_VIOLATION", result.Errors[0].Code);
        Assert.Contains("duplicate value in batch", result.Errors[0].Message);
    }

    [Fact]
    public void UniqueIndex_PurgeIndex_RemovesConstraint()
    {
        _engine.ExecuteOne("upsert users {name: 'Alice', email: 'alice@test.com', age: 28}", "testdb");
        _engine.ExecuteOne("create index unique users.email", "testdb");

        _engine.ExecuteOne("purge index users.email", "testdb");

        // Now duplicates should be allowed (no index, no constraint)
        _engine.ExecuteOne("upsert users {name: 'Bob', email: 'alice@test.com', age: 35}", "testdb");
        var result = _engine.ExecuteOne("get users", "testdb");

        Assert.NotNull(result.Data);
        Assert.Equal(2, result.Data.Count);
    }

    [Fact]
    public void UniqueIndex_DescribeShowsUnique()
    {
        _engine.ExecuteOne("create index unique users.email", "testdb");

        var result = _engine.ExecuteOne("describe users", "testdb");

        Assert.NotNull(result.Schema);
        var emailCol = result.Schema.Columns.Find(c => c.Name == "email");
        Assert.NotNull(emailCol);
        Assert.True(emailCol.IsUnique);
        Assert.True(emailCol.Indexed);
    }

    [Fact]
    public void UniqueIndex_PersistsAfterReload()
    {
        _engine.ExecuteOne("upsert users {name: 'Alice', email: 'alice@test.com', age: 28}", "testdb");
        _engine.ExecuteOne("create index unique users.email", "testdb");

        // Reload engine
        _engine.Dispose();
        _disposed = true;
        _engine = new SproutEngine(_tempDir);
        _disposed = false;

        // Unique constraint should still be enforced
        var result = _engine.ExecuteOne("upsert users {name: 'Bob', email: 'alice@test.com', age: 35}", "testdb");

        Assert.NotNull(result.Errors);
        Assert.Equal("UNIQUE_VIOLATION", result.Errors[0].Code);
    }

    // ── Duplicate key B-Tree regression ─────────────────────

    [Fact]
    public void BTreeRemove_DuplicateKeys_NoGhostRows()
    {
        // Regression: BTreeHandle.Remove failed to find entries with duplicate keys
        // (e.g. bool columns) causing stale entries to accumulate on each upsert.
        // WHERE queries via B-Tree then returned the same row multiple times.

        _engine.ExecuteOne(
            "create table tenants (slug string 100, name string 100, active bool default 'true')",
            "testdb");
        _engine.ExecuteOne("create index tenants.active", "testdb");

        // Insert two rows
        _engine.ExecuteOne("upsert tenants {slug: 'demo', name: 'Demo'}", "testdb");
        _engine.ExecuteOne("upsert tenants {slug: 'qsp', name: 'QSP GmbH'}", "testdb");

        // Update row _id=1 many times (same value — triggers Remove+Insert on B-Tree each time)
        for (int i = 0; i < 15; i++)
            _engine.ExecuteOne($"upsert tenants {{_id: 1, name: 'Demo v{i}'}}", "testdb");

        // Without the fix, this returned 16+ rows (15x _id=1 + 1x _id=2)
        var result = _engine.ExecuteOne("get tenants where active = true", "testdb");

        Assert.Null(result.Errors);
        Assert.NotNull(result.Data);
        Assert.Equal(2, result.Data.Count);
    }

    [Fact]
    public void BTreeRemove_DuplicateKeys_ValueChange_NoGhostRows()
    {
        // Same regression but with value changes (true → false → true)
        _engine.ExecuteOne(
            "create table flags (name string 100, enabled bool default 'true')",
            "testdb");
        _engine.ExecuteOne("create index flags.enabled", "testdb");

        _engine.ExecuteOne("upsert flags {name: 'feature_a'}", "testdb");
        _engine.ExecuteOne("upsert flags {name: 'feature_b'}", "testdb");

        // Toggle feature_a enabled back and forth
        for (int i = 0; i < 10; i++)
        {
            _engine.ExecuteOne($"upsert flags {{_id: 1, enabled: false}}", "testdb");
            _engine.ExecuteOne($"upsert flags {{_id: 1, enabled: true}}", "testdb");
        }

        var trueResult = _engine.ExecuteOne("get flags where enabled = true", "testdb");
        var falseResult = _engine.ExecuteOne("get flags where enabled = false", "testdb");

        Assert.Equal(2, trueResult.Data?.Count);
        Assert.Equal(0, falseResult.Data?.Count);
    }

    // ── Helpers ───────────────────────────────────────────

    private void SeedUsers(int count)
    {
        for (int i = 0; i < count; i++)
            _engine.ExecuteOne($"upsert users {{name: 'User{i}', email: 'user{i}@test.com', age: {(i % 50) + 18}}}", "testdb");
    }
}

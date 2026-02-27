namespace SproutDB.Core.Tests;

public class DescribeTests : IDisposable
{
    private readonly string _tempDir;
    private readonly SproutEngine _engine;

    public DescribeTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), $"sproutdb-test-{Guid.NewGuid()}");
        _engine = new SproutEngine(_tempDir);
        _engine.Execute("create database", "testdb");
        _engine.Execute("create table users (name string 100, age ubyte, email string 320)", "testdb");
    }

    public void Dispose()
    {
        _engine.Dispose();
        if (Directory.Exists(_tempDir))
            Directory.Delete(_tempDir, true);
    }

    // ── Describe Table ────────────────────────────────────────

    [Fact]
    public void DescribeTable_ReturnsColumns()
    {
        var r = _engine.Execute("describe users", "testdb");

        Assert.Equal(SproutOperation.Describe, r.Operation);
        Assert.Null(r.Errors);
        Assert.Equal("users", r.Schema?.Table);

        var columns = r.Schema?.Columns;
        Assert.NotNull(columns);
        Assert.Equal(4, columns.Count); // id + name + age + email
    }

    [Fact]
    public void DescribeTable_IdColumn()
    {
        var r = _engine.Execute("describe users", "testdb");
        var id = r.Schema?.Columns?.Find(c => c.Name == "id");

        Assert.NotNull(id);
        Assert.Equal("ulong", id.Type);
        Assert.False(id.Nullable);
        Assert.True(id.Strict);
        Assert.True(id.Auto);
    }

    [Fact]
    public void DescribeTable_StringColumn()
    {
        var r = _engine.Execute("describe users", "testdb");
        var name = r.Schema?.Columns?.Find(c => c.Name == "name");

        Assert.NotNull(name);
        Assert.Equal("string", name.Type);
        Assert.Equal(100, name.Size);
        Assert.True(name.Nullable); // no default → nullable
    }

    [Fact]
    public void DescribeTable_NullableColumn()
    {
        var r = _engine.Execute("describe users", "testdb");
        var email = r.Schema?.Columns?.Find(c => c.Name == "email");

        Assert.NotNull(email);
        Assert.Equal("string", email.Type);
        Assert.Equal(320, email.Size);
        Assert.True(email.Nullable); // no default → nullable
    }

    [Fact]
    public void DescribeTable_NumericColumn()
    {
        var r = _engine.Execute("describe users", "testdb");
        var age = r.Schema?.Columns?.Find(c => c.Name == "age");

        Assert.NotNull(age);
        Assert.Equal("ubyte", age.Type);
        Assert.Null(age.Size);
        Assert.True(age.Nullable); // no default → nullable
    }

    [Fact]
    public void DescribeTable_NonNullableColumn()
    {
        _engine.Execute("create table prefs (theme string 50 default 'dark')", "testdb");
        var r = _engine.Execute("describe prefs", "testdb");
        var theme = r.Schema?.Columns?.Find(c => c.Name == "theme");

        Assert.NotNull(theme);
        Assert.False(theme.Nullable); // has default → not nullable
        Assert.Equal("dark", theme.Default);
    }

    [Fact]
    public void DescribeTable_CaseInsensitive()
    {
        var r = _engine.Execute("DESCRIBE Users", "testdb");

        Assert.Equal(SproutOperation.Describe, r.Operation);
        Assert.Equal("users", r.Schema?.Table);
    }

    [Fact]
    public void DescribeTable_UnknownTable_Error()
    {
        var r = _engine.Execute("describe missing", "testdb");

        Assert.Equal(SproutOperation.Error, r.Operation);
        Assert.Equal("UNKNOWN_TABLE", r.Errors?[0].Code);
    }

    [Fact]
    public void DescribeTable_UnknownDatabase_Error()
    {
        var r = _engine.Execute("describe users", "nope");

        Assert.Equal(SproutOperation.Error, r.Operation);
        Assert.Equal("UNKNOWN_DATABASE", r.Errors?[0].Code);
    }

    // ── Describe All ──────────────────────────────────────────

    [Fact]
    public void DescribeAll_ReturnsTables()
    {
        _engine.Execute("create table orders (amount uint)", "testdb");

        var r = _engine.Execute("describe", "testdb");

        Assert.Equal(SproutOperation.Describe, r.Operation);
        Assert.Null(r.Errors);

        var tables = r.Schema?.Tables;
        Assert.NotNull(tables);
        Assert.Equal(2, tables.Count);
        Assert.Contains("users", tables);
        Assert.Contains("orders", tables);
    }

    [Fact]
    public void DescribeAll_EmptyDatabase()
    {
        _engine.Execute("create database", "emptydb");

        var r = _engine.Execute("describe", "emptydb");

        Assert.Equal(SproutOperation.Describe, r.Operation);
        Assert.NotNull(r.Schema?.Tables);
        Assert.Empty(r.Schema.Tables);
    }

    [Fact]
    public void DescribeAll_Sorted()
    {
        _engine.Execute("create table zebra (x ubyte)", "testdb");
        _engine.Execute("create table alpha (x ubyte)", "testdb");

        var r = _engine.Execute("describe", "testdb");
        var tables = r.Schema?.Tables;

        Assert.NotNull(tables);
        // alpha, users, zebra — sorted
        Assert.Equal("alpha", tables[0]);
        Assert.Equal("users", tables[1]);
        Assert.Equal("zebra", tables[2]);
    }

    [Fact]
    public void DescribeAll_UnknownDatabase_Error()
    {
        var r = _engine.Execute("describe", "nope");

        Assert.Equal(SproutOperation.Error, r.Operation);
        Assert.Equal("UNKNOWN_DATABASE", r.Errors?[0].Code);
    }

    // ── After schema changes ──────────────────────────────────

    [Fact]
    public void DescribeTable_AfterAddColumn()
    {
        _engine.Execute("add column users.active bool", "testdb");

        var r = _engine.Execute("describe users", "testdb");
        var active = r.Schema?.Columns?.Find(c => c.Name == "active");

        Assert.NotNull(active);
        Assert.Equal("bool", active.Type);
    }

    [Fact]
    public void DescribeTable_AfterPurgeColumn()
    {
        _engine.Execute("purge column users.age", "testdb");

        var r = _engine.Execute("describe users", "testdb");
        Assert.Null(r.Schema?.Columns?.Find(c => c.Name == "age"));
        Assert.Equal(3, r.Schema?.Columns?.Count); // id + name + email
    }

    [Fact]
    public void DescribeTable_AfterRenameColumn()
    {
        _engine.Execute("rename column users.name to username", "testdb");

        var r = _engine.Execute("describe users", "testdb");
        Assert.Null(r.Schema?.Columns?.Find(c => c.Name == "name"));
        Assert.NotNull(r.Schema?.Columns?.Find(c => c.Name == "username"));
    }

    [Fact]
    public void DescribeTable_AfterAlterColumn()
    {
        _engine.Execute("alter column users.name string 500", "testdb");

        var r = _engine.Execute("describe users", "testdb");
        var name = r.Schema?.Columns?.Find(c => c.Name == "name");

        Assert.NotNull(name);
        Assert.Equal(500, name.Size);
    }

    [Fact]
    public void DescribeAll_AfterPurgeTable()
    {
        _engine.Execute("purge table users", "testdb");

        var r = _engine.Execute("describe", "testdb");
        Assert.NotNull(r.Schema?.Tables);
        Assert.Empty(r.Schema.Tables);
    }
}

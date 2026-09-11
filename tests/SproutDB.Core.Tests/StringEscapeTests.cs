namespace SproutDB.Core.Tests;

/// <summary>
/// String literal escapes: <c>\'</c> → <c>'</c>, <c>\\</c> → <c>\</c>,
/// any other backslash stays literal.
/// </summary>
public class StringEscapeTests : IDisposable
{
    private class Item : ISproutEntity
    {
        public ulong Id { get; set; }
        public string? Name { get; set; }
    }

    private readonly string _tempDir;
    private readonly SproutEngine _engine;
    private readonly ISproutDatabase _db;

    public StringEscapeTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), $"sproutdb-test-{Guid.NewGuid()}");
        _engine = new SproutEngine(_tempDir);
        _db = _engine.GetOrCreateDatabase("testdb");
        _db.Query("create table items (name string 100)");
    }

    public void Dispose()
    {
        _engine.Dispose();
        if (Directory.Exists(_tempDir))
            Directory.Delete(_tempDir, true);
    }

    private string? StoredName()
    {
        var r = _engine.ExecuteOne("get items select name", "testdb");
        var row = Assert.Single(r.Data ?? []);
        return row["name"] as string;
    }

    [Theory]
    [InlineData(@"'a\\'", @"a\")]           // trailing backslash
    [InlineData(@"'a\\b'", @"a\b")]         // escaped backslash
    [InlineData(@"'O\'Brien'", "O'Brien")]  // escaped quote
    [InlineData(@"'a\\\'b'", @"a\'b")]      // backslash followed by quote
    [InlineData(@"'C:\temp'", @"C:\temp")]  // lone backslash stays literal
    [InlineData(@"'\\\\server'", @"\\server")]
    public void Upsert_StringLiteral_IsUnescaped(string literal, string expected)
    {
        var r = _engine.ExecuteOne($"upsert items {{name: {literal}}}", "testdb");
        Assert.Null(r.Errors);
        Assert.Equal(expected, StoredName());
    }

    [Fact]
    public void Where_TrailingBackslash_Matches()
    {
        _engine.ExecuteOne(@"upsert items {name: 'a\\'}", "testdb");

        var r = _engine.ExecuteOne(@"get items where name = 'a\\'", "testdb");
        Assert.Single(r.Data ?? []);
    }

    [Theory]
    [InlineData(@"a\")]
    [InlineData(@"a\\b")]
    [InlineData(@"O'Brien\")]
    [InlineData(@"\'")]
    public void TypedApi_RoundTripsBackslashesAndQuotes(string name)
    {
        var items = _db.Table<Item>("items");
        items.Upsert(new Item { Name = name });

        Assert.Equal(name, StoredName());

        var found = items.Where(i => i.Name == name).ToList();
        var item = Assert.Single(found);
        Assert.Equal(name, item.Name);
    }

    [Fact]
    public void SaveQuery_WithBackslashAndQuote_RoundTrips()
    {
        const string query = @"get items where name = 'a\\\'b'";
        _db.SaveQuery("q", query);

        var r = _engine.ExecuteOne("get _saved_queries select query where name = 'q'", "testdb");
        var row = Assert.Single(r.Data ?? []);
        Assert.Equal(query, row["query"]);
    }
}

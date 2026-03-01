using SproutDB.Core.Parsing;

namespace SproutDB.Core.Tests.Parsing;

public class AddColumnParserTests
{
    // ── Success cases ────────────────────────────────────────

    [Fact]
    public void SimpleColumn()
    {
        var result = QueryParser.Parse("add column users.premium bool");

        Assert.True(result.Success);
        var q = Assert.IsType<AddColumnQuery>(result.Query);
        Assert.Equal("users", q.Table);
        Assert.Equal("premium", q.Column.Name);
        Assert.Equal(ColumnType.Bool, q.Column.Type);
        Assert.True(q.Column.IsNullable);
        Assert.False(q.Column.Strict);
        Assert.Null(q.Column.Default);
    }

    [Fact]
    public void WithDefault()
    {
        var result = QueryParser.Parse("add column orders.priority sint default 0");
        var q = Assert.IsType<AddColumnQuery>(result.Query);

        Assert.Equal("orders", q.Table);
        Assert.Equal("priority", q.Column.Name);
        Assert.Equal(ColumnType.SInt, q.Column.Type);
        Assert.Equal("0", q.Column.Default);
        Assert.False(q.Column.IsNullable); // has default → not nullable
    }

    [Fact]
    public void WithStrict()
    {
        var result = QueryParser.Parse("add column users.nickname string strict");
        var q = Assert.IsType<AddColumnQuery>(result.Query);

        Assert.True(q.Column.Strict);
        Assert.Equal(ColumnType.String, q.Column.Type);
        Assert.Equal(255, q.Column.Size);
    }

    [Fact]
    public void StringWithSize()
    {
        var result = QueryParser.Parse("add column users.bio string 5000");
        var q = Assert.IsType<AddColumnQuery>(result.Query);

        Assert.Equal(5000, q.Column.Size);
    }

    [Fact]
    public void DefaultBeforeStrict()
    {
        var result = QueryParser.Parse("add column t.x sint default 42 strict");
        var q = Assert.IsType<AddColumnQuery>(result.Query);

        Assert.Equal("42", q.Column.Default);
        Assert.True(q.Column.Strict);
    }

    [Fact]
    public void StrictBeforeDefault()
    {
        var result = QueryParser.Parse("add column t.x ubyte strict default 0");
        var q = Assert.IsType<AddColumnQuery>(result.Query);

        Assert.True(q.Column.Strict);
        Assert.Equal("0", q.Column.Default);
    }

    [Fact]
    public void CaseInsensitive()
    {
        var result = QueryParser.Parse("ADD COLUMN Users.Name STRING");

        Assert.True(result.Success);
        var q = Assert.IsType<AddColumnQuery>(result.Query);
        Assert.Equal("users", q.Table);
        Assert.Equal("name", q.Column.Name);
    }

    [Fact]
    public void BoolDefault()
    {
        var result = QueryParser.Parse("add column t.active bool default true");
        var q = Assert.IsType<AddColumnQuery>(result.Query);

        Assert.Equal("true", q.Column.Default);
    }

    [Fact]
    public void StringDefault()
    {
        var result = QueryParser.Parse("add column t.status string default 'pending'");
        var q = Assert.IsType<AddColumnQuery>(result.Query);

        Assert.Equal("pending", q.Column.Default);
    }

    [Fact]
    public void NegativeDefault()
    {
        var result = QueryParser.Parse("add column t.offset sshort default -100");
        var q = Assert.IsType<AddColumnQuery>(result.Query);

        Assert.Equal("-100", q.Column.Default);
    }

    // ── Error cases ──────────────────────────────────────────

    [Fact]
    public void MissingColumnKeyword_Error()
    {
        var result = QueryParser.Parse("add users.name string");
        Assert.False(result.Success);
        Assert.Contains("expected 'column'", result.Errors![0].Message);
    }

    [Fact]
    public void MissingDot_Error()
    {
        var result = QueryParser.Parse("add column users name string");
        Assert.False(result.Success);
        Assert.Contains("expected '.'", result.Errors![0].Message);
    }

    [Fact]
    public void MissingColumnName_Error()
    {
        var result = QueryParser.Parse("add column users.");
        Assert.False(result.Success);
    }

    [Fact]
    public void MissingType_Error()
    {
        var result = QueryParser.Parse("add column users.name");
        Assert.False(result.Success);
        Assert.Contains("expected column type", result.Errors![0].Message);
    }

    [Fact]
    public void ReservedIdColumn_Error()
    {
        var result = QueryParser.Parse("add column users._id ulong");
        Assert.False(result.Success);
        Assert.Contains("'_id' is reserved", result.Errors![0].Message);
    }

    [Fact]
    public void MissingDefaultValue_Error()
    {
        var result = QueryParser.Parse("add column users.x sint default");
        Assert.False(result.Success);
        Assert.Contains("expected default value", result.Errors![0].Message);
    }
}

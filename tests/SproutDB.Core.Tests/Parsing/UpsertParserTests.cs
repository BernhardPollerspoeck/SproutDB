using SproutDB.Core.Parsing;

namespace SproutDB.Core.Tests.Parsing;

public class UpsertParserTests
{
    // ── Success cases ────────────────────────────────────────

    [Fact]
    public void SimpleInsert()
    {
        var result = QueryParser.Parse("upsert users {name: 'John', age: 25}");

        Assert.True(result.Success);
        var q = Assert.IsType<UpsertQuery>(result.Query);
        Assert.Equal("users", q.Table);
        Assert.Equal(2, q.Fields.Count);

        Assert.Equal("name", q.Fields[0].Name);
        Assert.Equal(UpsertValueKind.String, q.Fields[0].Value.Kind);
        Assert.Equal("John", q.Fields[0].Value.Raw);

        Assert.Equal("age", q.Fields[1].Name);
        Assert.Equal(UpsertValueKind.Integer, q.Fields[1].Value.Kind);
        Assert.Equal("25", q.Fields[1].Value.Raw);
    }

    [Fact]
    public void EmptyObject()
    {
        var result = QueryParser.Parse("upsert users {}");

        Assert.True(result.Success);
        var q = Assert.IsType<UpsertQuery>(result.Query);
        Assert.Equal("users", q.Table);
        Assert.Empty(q.Fields);
    }

    [Fact]
    public void WithExplicitId()
    {
        var result = QueryParser.Parse("upsert users {id: 42, name: 'John'}");

        Assert.True(result.Success);
        var q = Assert.IsType<UpsertQuery>(result.Query);
        Assert.Equal(2, q.Fields.Count);

        Assert.Equal("id", q.Fields[0].Name);
        Assert.Equal(UpsertValueKind.Integer, q.Fields[0].Value.Kind);
        Assert.Equal("42", q.Fields[0].Value.Raw);
    }

    [Fact]
    public void NullValue()
    {
        var result = QueryParser.Parse("upsert users {id: 1, email: null}");

        Assert.True(result.Success);
        var q = Assert.IsType<UpsertQuery>(result.Query);
        Assert.Equal("email", q.Fields[1].Name);
        Assert.Equal(UpsertValueKind.Null, q.Fields[1].Value.Kind);
        Assert.Null(q.Fields[1].Value.Raw);
    }

    [Fact]
    public void BooleanValues()
    {
        var result = QueryParser.Parse("upsert users {active: true, deleted: false}");

        Assert.True(result.Success);
        var q = Assert.IsType<UpsertQuery>(result.Query);

        Assert.Equal(UpsertValueKind.Boolean, q.Fields[0].Value.Kind);
        Assert.Equal("true", q.Fields[0].Value.Raw);

        Assert.Equal(UpsertValueKind.Boolean, q.Fields[1].Value.Kind);
        Assert.Equal("false", q.Fields[1].Value.Raw);
    }

    [Fact]
    public void NegativeInteger()
    {
        var result = QueryParser.Parse("upsert t {temp: -10}");

        Assert.True(result.Success);
        var q = Assert.IsType<UpsertQuery>(result.Query);
        Assert.Equal(UpsertValueKind.Integer, q.Fields[0].Value.Kind);
        Assert.Equal("-10", q.Fields[0].Value.Raw);
    }

    [Fact]
    public void FloatValue()
    {
        var result = QueryParser.Parse("upsert t {rate: 3.14}");

        Assert.True(result.Success);
        var q = Assert.IsType<UpsertQuery>(result.Query);
        Assert.Equal(UpsertValueKind.Float, q.Fields[0].Value.Kind);
        Assert.Equal("3.14", q.Fields[0].Value.Raw);
    }

    [Fact]
    public void NegativeFloat()
    {
        var result = QueryParser.Parse("upsert t {temp: -0.5}");

        Assert.True(result.Success);
        var q = Assert.IsType<UpsertQuery>(result.Query);
        Assert.Equal(UpsertValueKind.Float, q.Fields[0].Value.Kind);
        Assert.Equal("-0.5", q.Fields[0].Value.Raw);
    }

    [Fact]
    public void CaseInsensitive()
    {
        var result = QueryParser.Parse("UPSERT Users {Name: 'John'}");

        Assert.True(result.Success);
        var q = Assert.IsType<UpsertQuery>(result.Query);
        Assert.Equal("users", q.Table);
        Assert.Equal("name", q.Fields[0].Name);
    }

    [Fact]
    public void TableName_IsLowercased()
    {
        var q = Assert.IsType<UpsertQuery>(QueryParser.Parse("upsert MyTable {x: 1}").Query);
        Assert.Equal("mytable", q.Table);
    }

    // ── Error cases ──────────────────────────────────────────

    [Fact]
    public void MissingTableName_Error()
    {
        var result = QueryParser.Parse("upsert");
        Assert.False(result.Success);
        Assert.Contains("expected table name", result.Errors![0].Message);
    }

    [Fact]
    public void MissingBrace_Error()
    {
        var result = QueryParser.Parse("upsert users");
        Assert.False(result.Success);
        Assert.Contains("expected '{'", result.Errors![0].Message);
    }

    [Fact]
    public void MissingColon_Error()
    {
        var result = QueryParser.Parse("upsert users {name 'John'}");
        Assert.False(result.Success);
        Assert.Contains("expected ':'", result.Errors![0].Message);
    }

    [Fact]
    public void MissingValue_Error()
    {
        var result = QueryParser.Parse("upsert users {name:}");
        Assert.False(result.Success);
        Assert.Contains("expected a value", result.Errors![0].Message);
    }

    [Fact]
    public void MissingClosingBrace_Error()
    {
        var result = QueryParser.Parse("upsert users {name: 'John'");
        Assert.False(result.Success);
    }

    [Fact]
    public void TrailingTokens_Error()
    {
        var result = QueryParser.Parse("upsert users {name: 'John'} extra");
        Assert.False(result.Success);
        Assert.Contains("unexpected token", result.Errors![0].Message);
    }
}

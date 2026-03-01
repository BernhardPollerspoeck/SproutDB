using SproutDB.Core.Parsing;

namespace SproutDB.Core.Tests.Parsing;

public class UpsertParserTests
{
    // Helper to get fields of a single-record upsert
    private static List<UpsertField> Fields(UpsertQuery q) => q.Records[0];

    // ── Success cases ────────────────────────────────────────

    [Fact]
    public void SimpleInsert()
    {
        var result = QueryParser.Parse("upsert users {name: 'John', age: 25}");

        Assert.True(result.Success);
        var q = Assert.IsType<UpsertQuery>(result.Query);
        Assert.Equal("users", q.Table);
        Assert.Single(q.Records);
        Assert.Equal(2, Fields(q).Count);

        Assert.Equal("name", Fields(q)[0].Name);
        Assert.Equal(UpsertValueKind.String, Fields(q)[0].Value.Kind);
        Assert.Equal("John", Fields(q)[0].Value.Raw);

        Assert.Equal("age", Fields(q)[1].Name);
        Assert.Equal(UpsertValueKind.Integer, Fields(q)[1].Value.Kind);
        Assert.Equal("25", Fields(q)[1].Value.Raw);
    }

    [Fact]
    public void EmptyObject()
    {
        var result = QueryParser.Parse("upsert users {}");

        Assert.True(result.Success);
        var q = Assert.IsType<UpsertQuery>(result.Query);
        Assert.Equal("users", q.Table);
        Assert.Single(q.Records);
        Assert.Empty(Fields(q));
    }

    [Fact]
    public void WithExplicitId()
    {
        var result = QueryParser.Parse("upsert users {_id: 42, name: 'John'}");

        Assert.True(result.Success);
        var q = Assert.IsType<UpsertQuery>(result.Query);
        Assert.Equal(2, Fields(q).Count);

        Assert.Equal("_id", Fields(q)[0].Name);
        Assert.Equal(UpsertValueKind.Integer, Fields(q)[0].Value.Kind);
        Assert.Equal("42", Fields(q)[0].Value.Raw);
    }

    [Fact]
    public void NullValue()
    {
        var result = QueryParser.Parse("upsert users {_id: 1, email: null}");

        Assert.True(result.Success);
        var q = Assert.IsType<UpsertQuery>(result.Query);
        Assert.Equal("email", Fields(q)[1].Name);
        Assert.Equal(UpsertValueKind.Null, Fields(q)[1].Value.Kind);
        Assert.Null(Fields(q)[1].Value.Raw);
    }

    [Fact]
    public void BooleanValues()
    {
        var result = QueryParser.Parse("upsert users {active: true, deleted: false}");

        Assert.True(result.Success);
        var q = Assert.IsType<UpsertQuery>(result.Query);

        Assert.Equal(UpsertValueKind.Boolean, Fields(q)[0].Value.Kind);
        Assert.Equal("true", Fields(q)[0].Value.Raw);

        Assert.Equal(UpsertValueKind.Boolean, Fields(q)[1].Value.Kind);
        Assert.Equal("false", Fields(q)[1].Value.Raw);
    }

    [Fact]
    public void NegativeInteger()
    {
        var result = QueryParser.Parse("upsert t {temp: -10}");

        Assert.True(result.Success);
        var q = Assert.IsType<UpsertQuery>(result.Query);
        Assert.Equal(UpsertValueKind.Integer, Fields(q)[0].Value.Kind);
        Assert.Equal("-10", Fields(q)[0].Value.Raw);
    }

    [Fact]
    public void FloatValue()
    {
        var result = QueryParser.Parse("upsert t {rate: 3.14}");

        Assert.True(result.Success);
        var q = Assert.IsType<UpsertQuery>(result.Query);
        Assert.Equal(UpsertValueKind.Float, Fields(q)[0].Value.Kind);
        Assert.Equal("3.14", Fields(q)[0].Value.Raw);
    }

    [Fact]
    public void NegativeFloat()
    {
        var result = QueryParser.Parse("upsert t {temp: -0.5}");

        Assert.True(result.Success);
        var q = Assert.IsType<UpsertQuery>(result.Query);
        Assert.Equal(UpsertValueKind.Float, Fields(q)[0].Value.Kind);
        Assert.Equal("-0.5", Fields(q)[0].Value.Raw);
    }

    [Fact]
    public void CaseInsensitive()
    {
        var result = QueryParser.Parse("UPSERT Users {Name: 'John'}");

        Assert.True(result.Success);
        var q = Assert.IsType<UpsertQuery>(result.Query);
        Assert.Equal("users", q.Table);
        Assert.Equal("name", Fields(q)[0].Name);
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

    // ── ON clause ─────────────────────────────────────────────

    [Fact]
    public void OnClause_Parsed()
    {
        var result = QueryParser.Parse("upsert users {email: 'john@test.com', name: 'John'} on email");

        Assert.True(result.Success);
        var q = Assert.IsType<UpsertQuery>(result.Query);
        Assert.Equal("email", q.OnColumn);
        Assert.Equal(2, Fields(q).Count);
    }

    [Fact]
    public void OnClause_CaseInsensitive()
    {
        var result = QueryParser.Parse("upsert users {email: 'john@test.com'} ON Email");

        Assert.True(result.Success);
        var q = Assert.IsType<UpsertQuery>(result.Query);
        Assert.Equal("email", q.OnColumn);
    }

    [Fact]
    public void NoOnClause_OnColumnIsNull()
    {
        var result = QueryParser.Parse("upsert users {name: 'John'}");

        Assert.True(result.Success);
        var q = Assert.IsType<UpsertQuery>(result.Query);
        Assert.Null(q.OnColumn);
    }

    [Fact]
    public void OnClause_MissingColumnName_Error()
    {
        var result = QueryParser.Parse("upsert users {name: 'John'} on");
        Assert.False(result.Success);
    }

    // ── Bulk syntax ───────────────────────────────────────────

    [Fact]
    public void Bulk_TwoRecords()
    {
        var result = QueryParser.Parse("upsert users [{name: 'John', age: 25}, {name: 'Jane', age: 30}]");

        Assert.True(result.Success);
        var q = Assert.IsType<UpsertQuery>(result.Query);
        Assert.Equal(2, q.Records.Count);

        Assert.Equal("John", q.Records[0][0].Value.Raw);
        Assert.Equal("Jane", q.Records[1][0].Value.Raw);
    }

    [Fact]
    public void Bulk_SingleRecord()
    {
        var result = QueryParser.Parse("upsert users [{name: 'John'}]");

        Assert.True(result.Success);
        var q = Assert.IsType<UpsertQuery>(result.Query);
        Assert.Single(q.Records);
    }

    [Fact]
    public void Bulk_WithOnClause()
    {
        var result = QueryParser.Parse("upsert users [{email: 'a@b.com'}, {email: 'c@d.com'}] on email");

        Assert.True(result.Success);
        var q = Assert.IsType<UpsertQuery>(result.Query);
        Assert.Equal(2, q.Records.Count);
        Assert.Equal("email", q.OnColumn);
    }

    [Fact]
    public void Bulk_MissingClosingBracket_Error()
    {
        var result = QueryParser.Parse("upsert users [{name: 'John'}");
        Assert.False(result.Success);
    }

    [Fact]
    public void Bulk_MissingBraceInArray_Error()
    {
        var result = QueryParser.Parse("upsert users [name: 'John']");
        Assert.False(result.Success);
    }
}

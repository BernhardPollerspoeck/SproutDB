using SproutDB.Core.Parsing;

namespace SproutDB.Core.Tests.Parsing;

public class GetParserTests
{
    [Fact]
    public void GetAll_Success()
    {
        var result = QueryParser.Parse("get users");

        Assert.True(result.Success);
        var q = Assert.IsType<GetQuery>(result.Query);
        Assert.Equal(SproutOperation.Get, q.Operation);
        Assert.Equal("users", q.Table);
        Assert.Null(q.Select);
    }

    [Fact]
    public void GetAll_CaseInsensitive()
    {
        var result = QueryParser.Parse("GET Users");

        Assert.True(result.Success);
        var q = Assert.IsType<GetQuery>(result.Query);
        Assert.Equal("users", q.Table);
    }

    [Fact]
    public void GetWithSelect_SingleColumn()
    {
        var result = QueryParser.Parse("get users select name");

        Assert.True(result.Success);
        var q = Assert.IsType<GetQuery>(result.Query);
        Assert.NotNull(q.Select);
        Assert.Single(q.Select);
        Assert.Equal("name", q.Select[0]);
    }

    [Fact]
    public void GetWithSelect_MultipleColumns()
    {
        var result = QueryParser.Parse("get users select name, email, age");

        Assert.True(result.Success);
        var q = Assert.IsType<GetQuery>(result.Query);
        Assert.NotNull(q.Select);
        Assert.Equal(3, q.Select.Count);
        Assert.Equal("name", q.Select[0]);
        Assert.Equal("email", q.Select[1]);
        Assert.Equal("age", q.Select[2]);
    }

    [Fact]
    public void GetWithSelect_IncludesId()
    {
        var result = QueryParser.Parse("get users select id, name");

        Assert.True(result.Success);
        var q = Assert.IsType<GetQuery>(result.Query);
        Assert.NotNull(q.Select);
        Assert.Equal(2, q.Select.Count);
        Assert.Equal("id", q.Select[0]);
        Assert.Equal("name", q.Select[1]);
    }

    [Fact]
    public void GetWithSelect_ColumnNamesLowercased()
    {
        var result = QueryParser.Parse("get users select Name, EMAIL");

        Assert.True(result.Success);
        var q = Assert.IsType<GetQuery>(result.Query);
        Assert.Equal("name", q.Select![0]);
        Assert.Equal("email", q.Select[1]);
    }

    [Fact]
    public void GetWithComment_Success()
    {
        var result = QueryParser.Parse("get users ##all users##");

        Assert.True(result.Success);
        var q = Assert.IsType<GetQuery>(result.Query);
        Assert.Equal("users", q.Table);
        Assert.Null(q.Select);
    }

    [Fact]
    public void GetWithSelect_AndComment_Success()
    {
        var result = QueryParser.Parse("get users select name ##just names##");

        Assert.True(result.Success);
        var q = Assert.IsType<GetQuery>(result.Query);
        Assert.Single(q.Select!);
        Assert.Equal("name", q.Select![0]);
    }

    // ── Error cases ───────────────────────────────────────────

    [Fact]
    public void Get_MissingTableName_Error()
    {
        var result = QueryParser.Parse("get");

        Assert.False(result.Success);
        Assert.Equal("SYNTAX_ERROR", result.Errors![0].Code);
        Assert.Contains("expected table name", result.Errors[0].Message);
    }

    [Fact]
    public void Get_SelectWithoutColumns_Error()
    {
        var result = QueryParser.Parse("get users select");

        Assert.False(result.Success);
        Assert.Equal("SYNTAX_ERROR", result.Errors![0].Code);
        Assert.Contains("expected column name", result.Errors[0].Message);
    }

    [Fact]
    public void Get_TrailingCommaInSelect_Error()
    {
        var result = QueryParser.Parse("get users select name,");

        Assert.False(result.Success);
        Assert.Equal("SYNTAX_ERROR", result.Errors![0].Code);
    }

    [Fact]
    public void Get_ExtraTokensAfterSelect_Error()
    {
        var result = QueryParser.Parse("get users select name extra");

        Assert.False(result.Success);
        Assert.Contains("unexpected token", result.Errors![0].Message);
    }
}

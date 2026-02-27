using SproutDB.Core.Parsing;

namespace SproutDB.Core.Tests.Parsing;

public class RenameColumnParserTests
{
    [Fact]
    public void Success()
    {
        var result = QueryParser.Parse("rename column users.name to username");

        Assert.True(result.Success);
        var q = Assert.IsType<RenameColumnQuery>(result.Query);
        Assert.Equal("users", q.Table);
        Assert.Equal("name", q.OldColumn);
        Assert.Equal("username", q.NewColumn);
    }

    [Fact]
    public void CaseInsensitive()
    {
        var result = QueryParser.Parse("RENAME COLUMN Users.Name TO Username");

        Assert.True(result.Success);
        var q = Assert.IsType<RenameColumnQuery>(result.Query);
        Assert.Equal("users", q.Table);
        Assert.Equal("name", q.OldColumn);
        Assert.Equal("username", q.NewColumn);
    }

    [Fact]
    public void MissingColumnKeyword_Error()
    {
        var result = QueryParser.Parse("rename users.name to username");

        Assert.False(result.Success);
        Assert.Contains("expected 'column'", result.Errors![0].Message);
    }

    [Fact]
    public void MissingDot_Error()
    {
        var result = QueryParser.Parse("rename column users name to username");

        Assert.False(result.Success);
        Assert.Contains("expected '.'", result.Errors![0].Message);
    }

    [Fact]
    public void MissingToKeyword_Error()
    {
        var result = QueryParser.Parse("rename column users.name username");

        Assert.False(result.Success);
        Assert.Contains("expected 'to'", result.Errors![0].Message);
    }

    [Fact]
    public void MissingNewName_Error()
    {
        var result = QueryParser.Parse("rename column users.name to");

        Assert.False(result.Success);
    }

    [Fact]
    public void OldNameId_Error()
    {
        var result = QueryParser.Parse("rename column users.id to newid");

        Assert.False(result.Success);
        Assert.Contains("'id' is reserved", result.Errors![0].Message);
    }

    [Fact]
    public void NewNameId_Error()
    {
        var result = QueryParser.Parse("rename column users.name to id");

        Assert.False(result.Success);
        Assert.Contains("'id' is reserved", result.Errors![0].Message);
    }

    [Fact]
    public void ExtraTokens_Error()
    {
        var result = QueryParser.Parse("rename column users.name to username extra");

        Assert.False(result.Success);
        Assert.Contains("expected end of query", result.Errors![0].Message);
    }
}

using SproutDB.Core.Parsing;

namespace SproutDB.Core.Tests.Parsing;

public class AlterColumnParserTests
{
    [Fact]
    public void Success()
    {
        var result = QueryParser.Parse("alter column users.bio string 10000");

        Assert.True(result.Success);
        var q = Assert.IsType<AlterColumnQuery>(result.Query);
        Assert.Equal("users", q.Table);
        Assert.Equal("bio", q.Column);
        Assert.Equal(10000, q.NewSize);
    }

    [Fact]
    public void CaseInsensitive()
    {
        var result = QueryParser.Parse("ALTER COLUMN Users.Name STRING 500");

        Assert.True(result.Success);
        var q = Assert.IsType<AlterColumnQuery>(result.Query);
        Assert.Equal("users", q.Table);
        Assert.Equal("name", q.Column);
        Assert.Equal(500, q.NewSize);
    }

    [Fact]
    public void MissingColumnKeyword_Error()
    {
        var result = QueryParser.Parse("alter users.bio string 10000");

        Assert.False(result.Success);
        Assert.Contains("expected 'column'", result.Errors![0].Message);
    }

    [Fact]
    public void MissingDot_Error()
    {
        var result = QueryParser.Parse("alter column users bio string 10000");

        Assert.False(result.Success);
        Assert.Contains("expected '.'", result.Errors![0].Message);
    }

    [Fact]
    public void MissingStringKeyword_Error()
    {
        var result = QueryParser.Parse("alter column users.bio 10000");

        Assert.False(result.Success);
        Assert.Contains("expected 'string'", result.Errors![0].Message);
    }

    [Fact]
    public void MissingSize_Error()
    {
        var result = QueryParser.Parse("alter column users.bio string");

        Assert.False(result.Success);
        Assert.Contains("expected size", result.Errors![0].Message);
    }

    [Fact]
    public void IdReserved_Error()
    {
        var result = QueryParser.Parse("alter column users._id string 100");

        Assert.False(result.Success);
        Assert.Contains("'_id' is reserved", result.Errors![0].Message);
    }

    [Fact]
    public void ExtraTokens_Error()
    {
        var result = QueryParser.Parse("alter column users.bio string 10000 extra");

        Assert.False(result.Success);
        Assert.Contains("expected end of query", result.Errors![0].Message);
    }
}

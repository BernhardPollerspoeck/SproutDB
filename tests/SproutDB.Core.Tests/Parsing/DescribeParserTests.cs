using SproutDB.Core.Parsing;

namespace SproutDB.Core.Tests.Parsing;

public class DescribeParserTests
{
    [Fact]
    public void DescribeTable_Success()
    {
        var result = QueryParser.Parse("describe users");

        Assert.True(result.Success);
        var q = Assert.IsType<DescribeQuery>(result.Query);
        Assert.Equal("users", q.Table);
    }

    [Fact]
    public void DescribeAll_Success()
    {
        var result = QueryParser.Parse("describe");

        Assert.True(result.Success);
        var q = Assert.IsType<DescribeQuery>(result.Query);
        Assert.Null(q.Table);
    }

    [Fact]
    public void CaseInsensitive()
    {
        var result = QueryParser.Parse("DESCRIBE Users");

        Assert.True(result.Success);
        var q = Assert.IsType<DescribeQuery>(result.Query);
        Assert.Equal("users", q.Table);
    }

    [Fact]
    public void ExtraTokens_Error()
    {
        var result = QueryParser.Parse("describe users extra");

        Assert.False(result.Success);
        Assert.Contains("expected end of query", result.Errors?[0].Message);
    }
}

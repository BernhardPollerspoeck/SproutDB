using SproutDB.Core.Parsing;

namespace SproutDB.Core.Tests.Parsing;

public class WhereParserTests
{
    [Fact]
    public void Where_Equal_Integer()
    {
        var result = QueryParser.Parse("get users where age = 28");

        Assert.True(result.Success);
        var q = Assert.IsType<GetQuery>(result.Query);
        Assert.Equal("users", q.Table);
        Assert.NotNull(q.Where);
        Assert.Equal("age", q.Where.Column);
        Assert.Equal(CompareOp.Equal, q.Where.Operator);
        Assert.Equal("28", q.Where.Value);
    }

    [Fact]
    public void Where_GreaterThan()
    {
        var result = QueryParser.Parse("get users where age > 30");

        Assert.True(result.Success);
        var q = Assert.IsType<GetQuery>(result.Query);
        Assert.Equal(CompareOp.GreaterThan, q.Where?.Operator);
        Assert.Equal("30", q.Where?.Value);
    }

    [Fact]
    public void Where_GreaterThanOrEqual()
    {
        var result = QueryParser.Parse("get users where age >= 18");

        Assert.True(result.Success);
        Assert.Equal(CompareOp.GreaterThanOrEqual, Assert.IsType<GetQuery>(result.Query).Where?.Operator);
    }

    [Fact]
    public void Where_LessThan()
    {
        var result = QueryParser.Parse("get users where age < 18");

        Assert.True(result.Success);
        Assert.Equal(CompareOp.LessThan, Assert.IsType<GetQuery>(result.Query).Where?.Operator);
    }

    [Fact]
    public void Where_LessThanOrEqual()
    {
        var result = QueryParser.Parse("get users where age <= 30");

        Assert.True(result.Success);
        Assert.Equal(CompareOp.LessThanOrEqual, Assert.IsType<GetQuery>(result.Query).Where?.Operator);
    }

    [Fact]
    public void Where_NotEqual()
    {
        var result = QueryParser.Parse("get users where age != 18");

        Assert.True(result.Success);
        Assert.Equal(CompareOp.NotEqual, Assert.IsType<GetQuery>(result.Query).Where?.Operator);
    }

    [Fact]
    public void Where_StringValue()
    {
        var result = QueryParser.Parse("get users where name = 'John'");

        Assert.True(result.Success);
        var q = Assert.IsType<GetQuery>(result.Query);
        Assert.Equal("name", q.Where?.Column);
        Assert.Equal("John", q.Where?.Value);
    }

    [Fact]
    public void Where_BoolValue()
    {
        var result = QueryParser.Parse("get users where active = true");

        Assert.True(result.Success);
        var q = Assert.IsType<GetQuery>(result.Query);
        Assert.Equal("true", q.Where?.Value);
    }

    [Fact]
    public void Where_NegativeValue()
    {
        var result = QueryParser.Parse("get users where score > -10");

        Assert.True(result.Success);
        var q = Assert.IsType<GetQuery>(result.Query);
        Assert.Equal("-10", q.Where?.Value);
    }

    [Fact]
    public void Where_FloatValue()
    {
        var result = QueryParser.Parse("get users where rating > 4.5");

        Assert.True(result.Success);
        var q = Assert.IsType<GetQuery>(result.Query);
        Assert.Equal("4.5", q.Where?.Value);
    }

    [Fact]
    public void Where_WithSelect()
    {
        var result = QueryParser.Parse("get users select name, age where age > 30");

        Assert.True(result.Success);
        var q = Assert.IsType<GetQuery>(result.Query);
        Assert.NotNull(q.Select);
        Assert.Equal(2, q.Select.Count);
        Assert.NotNull(q.Where);
        Assert.Equal("age", q.Where.Column);
    }

    [Fact]
    public void Where_CaseInsensitive()
    {
        var result = QueryParser.Parse("GET Users WHERE Age > 30");

        Assert.True(result.Success);
        var q = Assert.IsType<GetQuery>(result.Query);
        Assert.Equal("age", q.Where?.Column);
    }

    [Fact]
    public void NoWhere_NullClause()
    {
        var result = QueryParser.Parse("get users");

        Assert.True(result.Success);
        var q = Assert.IsType<GetQuery>(result.Query);
        Assert.Null(q.Where);
    }

    [Fact]
    public void Where_MissingOperator_Error()
    {
        var result = QueryParser.Parse("get users where age 30");

        Assert.False(result.Success);
        Assert.Contains("comparison operator", result.Errors![0].Message);
    }

    [Fact]
    public void Where_MissingValue_Error()
    {
        var result = QueryParser.Parse("get users where age >");

        Assert.False(result.Success);
        Assert.Contains("expected a value", result.Errors![0].Message);
    }

    [Fact]
    public void Where_MissingColumn_Error()
    {
        var result = QueryParser.Parse("get users where = 30");

        Assert.False(result.Success);
        Assert.Contains("column name", result.Errors![0].Message);
    }
}

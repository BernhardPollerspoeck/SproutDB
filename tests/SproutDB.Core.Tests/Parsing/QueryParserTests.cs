using SproutDB.Core.Parsing;

namespace SproutDB.Core.Tests.Parsing;

public class QueryParserTests
{
    [Fact]
    public void CreateDatabase_Success()
    {
        var result = QueryParser.Parse("create database");

        Assert.True(result.Success);
        Assert.IsType<CreateDatabaseQuery>(result.Query);
        Assert.Equal(SproutOperation.CreateDatabase, result.Query!.Operation);
        Assert.Null(result.Errors);
        Assert.Null(result.AnnotatedQuery);
    }

    [Fact]
    public void CreateDatabase_CaseInsensitive()
    {
        Assert.True(QueryParser.Parse("CREATE DATABASE").Success);
        Assert.True(QueryParser.Parse("Create Database").Success);
        Assert.True(QueryParser.Parse("cReAtE dAtAbAsE").Success);
    }

    [Fact]
    public void CreateDatabase_WithComment_Success()
    {
        var result = QueryParser.Parse("create database ##new db##");
        Assert.True(result.Success);
        Assert.IsType<CreateDatabaseQuery>(result.Query);
    }

    [Fact]
    public void CreateDatabase_WithTrailingComment_Success()
    {
        var result = QueryParser.Parse("create database ## setup");
        Assert.True(result.Success);
    }

    [Fact]
    public void EmptyInput_Error()
    {
        var result = QueryParser.Parse("");

        Assert.False(result.Success);
        Assert.Null(result.Query);
        Assert.NotNull(result.Errors);
        Assert.Single(result.Errors);
        Assert.Equal("SYNTAX_ERROR", result.Errors[0].Code);
        Assert.Contains("expected a command", result.Errors[0].Message);
    }

    [Fact]
    public void WhitespaceOnly_Error()
    {
        var result = QueryParser.Parse("   ");
        Assert.False(result.Success);
        Assert.Contains("expected a command", result.Errors![0].Message);
    }

    [Fact]
    public void UnknownCommand_Error()
    {
        var result = QueryParser.Parse("drop table users");

        Assert.False(result.Success);
        Assert.Equal("SYNTAX_ERROR", result.Errors![0].Code);
        Assert.Contains("unknown command", result.Errors[0].Message);
        Assert.NotNull(result.AnnotatedQuery);
        Assert.Contains("##", result.AnnotatedQuery);
    }

    [Fact]
    public void UnknownCommand_AnnotatedQuery()
    {
        var result = QueryParser.Parse("drop table users");

        // "drop" is annotated with the error
        Assert.Contains("drop ##", result.AnnotatedQuery);
    }

    [Fact]
    public void CreateWithoutSubcommand_Error()
    {
        var result = QueryParser.Parse("create");

        Assert.False(result.Success);
        Assert.Contains("expected 'database', 'table' or 'index'", result.Errors![0].Message);
    }

    [Fact]
    public void CreateWithWrongSubcommand_Error()
    {
        var result = QueryParser.Parse("create schema");

        Assert.False(result.Success);
        Assert.Contains("expected 'database', 'table' or 'index'", result.Errors![0].Message);
        Assert.Contains("schema ##", result.AnnotatedQuery!);
    }

    [Fact]
    public void CreateDatabase_ExtraTokens_Error()
    {
        var result = QueryParser.Parse("create database shop");

        Assert.False(result.Success);
        Assert.Contains("unexpected token", result.Errors![0].Message);
        Assert.Contains("end of query", result.Errors[0].Message);
        Assert.Contains("shop ##", result.AnnotatedQuery!);
    }

    [Fact]
    public void CreateDatabase_AnnotatedQuery_CorrectFormat()
    {
        var result = QueryParser.Parse("create database shop");

        // Should be: "create database shop ##unexpected token, expected end of query##"
        Assert.Equal(
            "create database shop ##unexpected token, expected end of query##",
            result.AnnotatedQuery);
    }
}

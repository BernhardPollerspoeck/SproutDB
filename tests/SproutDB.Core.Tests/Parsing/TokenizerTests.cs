using SproutDB.Core.Parsing;

namespace SproutDB.Core.Tests.Parsing;

public class TokenizerTests
{
    [Fact]
    public void EmptyInput_ReturnsOnlyEof()
    {
        var tokens = Tokenizer.Tokenize("");
        Assert.Single(tokens);
        Assert.Equal(TokenType.Eof, tokens[0].Type);
    }

    [Fact]
    public void WhitespaceOnly_ReturnsOnlyEof()
    {
        var tokens = Tokenizer.Tokenize("   \t  \n  ");
        Assert.Single(tokens);
        Assert.Equal(TokenType.Eof, tokens[0].Type);
    }

    [Fact]
    public void SingleIdentifier()
    {
        var tokens = Tokenizer.Tokenize("users");
        Assert.Equal(2, tokens.Count);
        Assert.Equal(TokenType.Identifier, tokens[0].Type);
        Assert.Equal("users", GetText("users", tokens[0]));
    }

    [Fact]
    public void MultipleIdentifiers()
    {
        var input = "create database";
        var tokens = Tokenizer.Tokenize(input);
        Assert.Equal(3, tokens.Count);
        Assert.Equal(TokenType.Identifier, tokens[0].Type);
        Assert.Equal("create", GetText(input, tokens[0]));
        Assert.Equal(TokenType.Identifier, tokens[1].Type);
        Assert.Equal("database", GetText(input, tokens[1]));
        Assert.Equal(TokenType.Eof, tokens[2].Type);
    }

    [Fact]
    public void IdentifierWithUnderscore()
    {
        var input = "_system next_id";
        var tokens = Tokenizer.Tokenize(input);
        Assert.Equal("_system", GetText(input, tokens[0]));
        Assert.Equal("next_id", GetText(input, tokens[1]));
    }

    [Fact]
    public void CaseIsPreserved()
    {
        var input = "CREATE Database";
        var tokens = Tokenizer.Tokenize(input);
        Assert.Equal("CREATE", GetText(input, tokens[0]));
        Assert.Equal("Database", GetText(input, tokens[1]));
    }

    [Fact]
    public void StringLiteral()
    {
        var input = "'hello world'";
        var tokens = Tokenizer.Tokenize(input);
        Assert.Equal(2, tokens.Count);
        Assert.Equal(TokenType.StringLiteral, tokens[0].Type);
        // Token includes quotes, start=0, length=13
        Assert.Equal(0, tokens[0].Start);
        Assert.Equal(13, tokens[0].Length);
    }

    [Fact]
    public void IntegerLiteral()
    {
        var input = "42";
        var tokens = Tokenizer.Tokenize(input);
        Assert.Equal(TokenType.IntegerLiteral, tokens[0].Type);
        Assert.Equal("42", GetText(input, tokens[0]));
    }

    [Fact]
    public void FloatLiteral()
    {
        var input = "3.14";
        var tokens = Tokenizer.Tokenize(input);
        Assert.Equal(TokenType.FloatLiteral, tokens[0].Type);
        Assert.Equal("3.14", GetText(input, tokens[0]));
    }

    [Fact]
    public void NegativeNumber_TokenizedAsTwoTokens()
    {
        var input = "-25";
        var tokens = Tokenizer.Tokenize(input);
        Assert.Equal(3, tokens.Count);
        Assert.Equal(TokenType.Minus, tokens[0].Type);
        Assert.Equal(TokenType.IntegerLiteral, tokens[1].Type);
        Assert.Equal("25", GetText(input, tokens[1]));
    }

    [Fact]
    public void AllDelimiters()
    {
        var input = "(){}[],:.*";
        var tokens = Tokenizer.Tokenize(input);
        Assert.Equal(TokenType.LeftParen, tokens[0].Type);
        Assert.Equal(TokenType.RightParen, tokens[1].Type);
        Assert.Equal(TokenType.LeftBrace, tokens[2].Type);
        Assert.Equal(TokenType.RightBrace, tokens[3].Type);
        Assert.Equal(TokenType.LeftBracket, tokens[4].Type);
        Assert.Equal(TokenType.RightBracket, tokens[5].Type);
        Assert.Equal(TokenType.Comma, tokens[6].Type);
        Assert.Equal(TokenType.Colon, tokens[7].Type);
        Assert.Equal(TokenType.Dot, tokens[8].Type);
        Assert.Equal(TokenType.Star, tokens[9].Type);
    }

    [Fact]
    public void ComparisonOperators()
    {
        var input = "= > < >= <= !=";
        var tokens = Tokenizer.Tokenize(input);
        Assert.Equal(TokenType.Equals, tokens[0].Type);
        Assert.Equal(TokenType.GreaterThan, tokens[1].Type);
        Assert.Equal(TokenType.LessThan, tokens[2].Type);
        Assert.Equal(TokenType.GreaterThanOrEqual, tokens[3].Type);
        Assert.Equal(TokenType.LessThanOrEqual, tokens[4].Type);
        Assert.Equal(TokenType.NotEqual, tokens[5].Type);
    }

    [Fact]
    public void Arrow()
    {
        var input = "users.id -> orders.user_id";
        var tokens = Tokenizer.Tokenize(input);
        Assert.Equal(TokenType.Identifier, tokens[0].Type); // users
        Assert.Equal(TokenType.Dot, tokens[1].Type);
        Assert.Equal(TokenType.Identifier, tokens[2].Type); // id
        Assert.Equal(TokenType.Arrow, tokens[3].Type);
        Assert.Equal(TokenType.Identifier, tokens[4].Type); // orders
        Assert.Equal(TokenType.Dot, tokens[5].Type);
        Assert.Equal(TokenType.Identifier, tokens[6].Type); // user_id
    }

    [Fact]
    public void InlineComment_IsSkipped()
    {
        var input = "get users ##all users## where active = true";
        var tokens = Tokenizer.Tokenize(input);
        // Tokens: get, users, where, active, =, true, eof
        Assert.Equal(7, tokens.Count);
        Assert.Equal("get", GetText(input, tokens[0]));
        Assert.Equal("users", GetText(input, tokens[1]));
        Assert.Equal("where", GetText(input, tokens[2]));
    }

    [Fact]
    public void TrailingComment_IsSkipped()
    {
        var input = "get users ## only active";
        var tokens = Tokenizer.Tokenize(input);
        // Tokens: get, users, eof
        Assert.Equal(3, tokens.Count);
        Assert.Equal("get", GetText(input, tokens[0]));
        Assert.Equal("users", GetText(input, tokens[1]));
        Assert.Equal(TokenType.Eof, tokens[2].Type);
    }

    [Fact]
    public void ComplexQuery_CorrectTokenCount()
    {
        var input = "upsert users {name: 'John', age: 25}";
        var tokens = Tokenizer.Tokenize(input);
        // upsert, users, {, name, :, 'John', ,, age, :, 25, }, eof
        Assert.Equal(12, tokens.Count);
        Assert.Equal(TokenType.Identifier, tokens[0].Type);
        Assert.Equal(TokenType.LeftBrace, tokens[2].Type);
        Assert.Equal(TokenType.Colon, tokens[4].Type);
        Assert.Equal(TokenType.StringLiteral, tokens[5].Type);
        Assert.Equal(TokenType.Comma, tokens[6].Type);
        Assert.Equal(TokenType.IntegerLiteral, tokens[9].Type);
        Assert.Equal(TokenType.RightBrace, tokens[10].Type);
    }

    [Fact]
    public void EofToken_PositionIsEndOfInput()
    {
        var input = "abc";
        var tokens = Tokenizer.Tokenize(input);
        var eof = tokens[^1];
        Assert.Equal(TokenType.Eof, eof.Type);
        Assert.Equal(3, eof.Start);
        Assert.Equal(0, eof.Length);
    }

    private static string GetText(string input, Token token)
        => input.AsSpan(token.Start, token.Length).ToString();
}

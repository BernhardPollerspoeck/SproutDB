namespace SproutDB.Core.Parsing;

/// <summary>
/// Parses: rotate apikey '&lt;name&gt;'
/// </summary>
internal static class RotateApiKeyParser
{
    public static ParseResult Parse(ParserContext ctx)
    {
        var nameToken = ctx.Peek();
        if (nameToken.Type != TokenType.StringLiteral)
            return ctx.Error(nameToken, ErrorCodes.SYNTAX_ERROR, "expected api key name as string literal");

        var name = ctx.GetStringLiteralText(nameToken);
        ctx.Advance();

        ctx.ExpectEof();
        if (ctx.HasErrors) return ctx.Fail();

        return ParseResult.Ok(new RotateApiKeyQuery { Name = name });
    }
}

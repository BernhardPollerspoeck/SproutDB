namespace SproutDB.Core.Parsing;

internal static class RestoreParser
{
    public static ParseResult Parse(ParserContext ctx)
    {
        // Expect a string literal with the file path
        var token = ctx.Peek();
        if (token.Type != TokenType.StringLiteral)
        {
            return ctx.Error(token, ErrorCodes.SYNTAX_ERROR, "expected backup file path as string literal");
        }

        var filePath = ctx.Input.Substring(token.Start + 1, token.Length - 2);
        ctx.Advance();

        ctx.ExpectEof();
        if (ctx.HasErrors) return ctx.Fail();

        return ParseResult.Ok(new RestoreQuery { FilePath = filePath });
    }
}

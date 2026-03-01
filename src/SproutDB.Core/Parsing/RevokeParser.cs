namespace SproutDB.Core.Parsing;

/// <summary>
/// Parses: revoke &lt;db&gt; from '&lt;name&gt;'
/// </summary>
internal static class RevokeParser
{
    public static ParseResult Parse(ParserContext ctx)
    {
        // database
        var dbToken = ctx.Peek();
        if (dbToken.Type != TokenType.Identifier)
            return ctx.Error(dbToken, ErrorCodes.SYNTAX_ERROR, "expected database name");

        var database = ctx.GetLowercaseText(dbToken);
        ctx.Advance();

        // from
        if (!ctx.MatchKeyword("from"))
            return ctx.Error(ctx.Peek(), ErrorCodes.SYNTAX_ERROR, "expected 'from'");

        // 'name'
        var nameToken = ctx.Peek();
        if (nameToken.Type != TokenType.StringLiteral)
            return ctx.Error(nameToken, ErrorCodes.SYNTAX_ERROR, "expected api key name as string literal");

        var keyName = ctx.GetStringLiteralText(nameToken);
        ctx.Advance();

        ctx.ExpectEof();
        if (ctx.HasErrors) return ctx.Fail();

        return ParseResult.Ok(new RevokeQuery
        {
            Database = database,
            KeyName = keyName,
        });
    }
}

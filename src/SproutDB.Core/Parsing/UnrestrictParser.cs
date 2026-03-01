namespace SproutDB.Core.Parsing;

/// <summary>
/// Parses: unrestrict &lt;table&gt; for '&lt;name&gt;' on &lt;db&gt;
/// </summary>
internal static class UnrestrictParser
{
    public static ParseResult Parse(ParserContext ctx)
    {
        // table
        var tableToken = ctx.Peek();
        if (tableToken.Type != TokenType.Identifier)
            return ctx.Error(tableToken, ErrorCodes.SYNTAX_ERROR, "expected table name");

        var table = ctx.GetLowercaseText(tableToken);
        ctx.Advance();

        // for
        if (!ctx.MatchKeyword("for"))
            return ctx.Error(ctx.Peek(), ErrorCodes.SYNTAX_ERROR, "expected 'for'");

        // 'name'
        var nameToken = ctx.Peek();
        if (nameToken.Type != TokenType.StringLiteral)
            return ctx.Error(nameToken, ErrorCodes.SYNTAX_ERROR, "expected api key name as string literal");

        var keyName = ctx.GetStringLiteralText(nameToken);
        ctx.Advance();

        // on
        if (!ctx.MatchKeyword("on"))
            return ctx.Error(ctx.Peek(), ErrorCodes.SYNTAX_ERROR, "expected 'on'");

        // database
        var dbToken = ctx.Peek();
        if (dbToken.Type != TokenType.Identifier)
            return ctx.Error(dbToken, ErrorCodes.SYNTAX_ERROR, "expected database name");

        var database = ctx.GetLowercaseText(dbToken);
        ctx.Advance();

        ctx.ExpectEof();
        if (ctx.HasErrors) return ctx.Fail();

        return ParseResult.Ok(new UnrestrictQuery
        {
            Table = table,
            KeyName = keyName,
            Database = database,
        });
    }
}

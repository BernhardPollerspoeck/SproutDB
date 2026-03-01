namespace SproutDB.Core.Parsing;

/// <summary>
/// Parses: restrict &lt;table|*&gt; to &lt;reader|none&gt; for '&lt;name&gt;' on &lt;db&gt;
/// </summary>
internal static class RestrictParser
{
    private static readonly HashSet<string> ValidRoles = new(StringComparer.OrdinalIgnoreCase)
    {
        "reader", "none",
    };

    public static ParseResult Parse(ParserContext ctx)
    {
        // table or *
        string table;
        var tableToken = ctx.Peek();
        if (tableToken.Type == TokenType.Star)
        {
            table = "*";
            ctx.Advance();
        }
        else if (tableToken.Type == TokenType.Identifier)
        {
            table = ctx.GetLowercaseText(tableToken);
            ctx.Advance();
        }
        else
        {
            return ctx.Error(tableToken, ErrorCodes.SYNTAX_ERROR, "expected table name or '*'");
        }

        // to
        if (!ctx.MatchKeyword("to"))
            return ctx.Error(ctx.Peek(), ErrorCodes.SYNTAX_ERROR, "expected 'to'");

        // role (reader or none)
        var roleToken = ctx.Peek();
        if (roleToken.Type != TokenType.Identifier)
            return ctx.Error(roleToken, ErrorCodes.SYNTAX_ERROR, "expected role (reader, none)");

        var role = ctx.GetLowercaseText(roleToken);
        if (!ValidRoles.Contains(role))
            return ctx.Error(roleToken, ErrorCodes.SYNTAX_ERROR, "expected role (reader, none)");
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

        return ParseResult.Ok(new RestrictQuery
        {
            Table = table,
            Role = role,
            KeyName = keyName,
            Database = database,
        });
    }
}

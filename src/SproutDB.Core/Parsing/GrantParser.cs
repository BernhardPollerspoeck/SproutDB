namespace SproutDB.Core.Parsing;

/// <summary>
/// Parses: grant &lt;role&gt; on &lt;db&gt; to '&lt;name&gt;'
/// </summary>
internal static class GrantParser
{
    private static readonly HashSet<string> ValidRoles = new(StringComparer.OrdinalIgnoreCase)
    {
        "admin", "writer", "reader",
    };

    public static ParseResult Parse(ParserContext ctx)
    {
        // role
        var roleToken = ctx.Peek();
        if (roleToken.Type != TokenType.Identifier)
            return ctx.Error(roleToken, ErrorCodes.SYNTAX_ERROR, "expected role (admin, writer, reader)");

        var role = ctx.GetLowercaseText(roleToken);
        if (!ValidRoles.Contains(role))
            return ctx.Error(roleToken, ErrorCodes.SYNTAX_ERROR, "expected role (admin, writer, reader)");
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

        // to
        if (!ctx.MatchKeyword("to"))
            return ctx.Error(ctx.Peek(), ErrorCodes.SYNTAX_ERROR, "expected 'to'");

        // 'name'
        var nameToken = ctx.Peek();
        if (nameToken.Type != TokenType.StringLiteral)
            return ctx.Error(nameToken, ErrorCodes.SYNTAX_ERROR, "expected api key name as string literal");

        var keyName = ctx.GetStringLiteralText(nameToken);
        ctx.Advance();

        ctx.ExpectEof();
        if (ctx.HasErrors) return ctx.Fail();

        return ParseResult.Ok(new GrantQuery
        {
            Role = role,
            Database = database,
            KeyName = keyName,
        });
    }
}

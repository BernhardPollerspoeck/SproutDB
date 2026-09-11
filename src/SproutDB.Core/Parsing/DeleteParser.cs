namespace SproutDB.Core.Parsing;

internal static class DeleteParser
{
    public static ParseResult Parse(ParserContext ctx)
    {
        // Table name
        var nameToken = ctx.Peek();
        if (nameToken.Type != TokenType.Identifier)
            return ctx.Error(nameToken, ErrorCodes.SYNTAX_ERROR, ErrorMessages.EXPECTED_TABLE_NAME);

        var tableName = ctx.GetLowercaseText(nameToken);
        ctx.Advance();

        // WHERE is required for delete
        if (!ctx.MatchKeyword("where"))
        {
            var current = ctx.Peek();
            return ctx.Error(current, ErrorCodes.WHERE_REQUIRED,
                "delete requires a 'where' clause");
        }

        var where = GetParser.ParseWhere(ctx);
        if (ctx.HasErrors) return ctx.Fail();

        if (where is null)
            return ctx.Error(ctx.Peek(), ErrorCodes.SYNTAX_ERROR, ErrorMessages.EXPECTED_COLUMN_NAME);

        // Optional EXPECT N (number of rows that must match)
        int? expect = null;
        if (ctx.MatchKeyword("expect"))
        {
            var countToken = ctx.Peek();
            if (countToken.Type != TokenType.IntegerLiteral
                || !int.TryParse(ctx.GetText(countToken), out var count))
            {
                return ctx.Error(countToken, ErrorCodes.SYNTAX_ERROR,
                    "expected a non-negative row count after 'expect'");
            }
            ctx.Advance();
            expect = count;
        }

        ctx.ExpectEof();
        if (ctx.HasErrors) return ctx.Fail();

        return ParseResult.Ok(new DeleteQuery
        {
            Table = tableName,
            Where = where,
            Expect = expect,
        });
    }
}

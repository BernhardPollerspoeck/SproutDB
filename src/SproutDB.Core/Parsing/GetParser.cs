namespace SproutDB.Core.Parsing;

internal static class GetParser
{
    public static ParseResult Parse(ParserContext ctx)
    {
        // Table name
        var nameToken = ctx.Peek();
        if (nameToken.Type != TokenType.Identifier)
            return ctx.Error(nameToken, ErrorCodes.SYNTAX_ERROR, ErrorMessages.EXPECTED_TABLE_NAME);

        var tableName = ctx.GetLowercaseText(nameToken);
        ctx.Advance();

        // Optional: select <col1>, <col2>, ...
        List<string>? selectColumns = null;

        if (ctx.Peek().Type != TokenType.Eof && ctx.MatchKeyword("select"))
        {
            selectColumns = ParseSelectList(ctx);
            if (ctx.HasErrors) return ctx.Fail();
        }

        ctx.ExpectEof();
        if (ctx.HasErrors) return ctx.Fail();

        return ParseResult.Ok(new GetQuery
        {
            Table = tableName,
            Select = selectColumns,
        });
    }

    private static List<string> ParseSelectList(ParserContext ctx)
    {
        var columns = new List<string>();

        while (true)
        {
            var token = ctx.Peek();
            if (token.Type != TokenType.Identifier)
            {
                ctx.AddError(token, ErrorCodes.SYNTAX_ERROR, ErrorMessages.EXPECTED_COLUMN_NAME);
                return columns;
            }

            columns.Add(ctx.GetLowercaseText(token));
            ctx.Advance();

            if (ctx.Peek().Type == TokenType.Comma)
            {
                ctx.Advance();
                continue;
            }

            break;
        }

        return columns;
    }
}

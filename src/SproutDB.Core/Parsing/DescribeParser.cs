namespace SproutDB.Core.Parsing;

internal static class DescribeParser
{
    public static ParseResult Parse(ParserContext ctx)
    {
        var next = ctx.Peek();

        // `describe` (no argument) → describe all tables
        if (next.Type == TokenType.Eof)
            return ParseResult.Ok(new DescribeQuery { Table = null });

        // `describe <tablename>` → describe single table
        if (next.Type != TokenType.Identifier)
            return ctx.Error(next, ErrorCodes.SYNTAX_ERROR, ErrorMessages.EXPECTED_TABLE_NAME);

        var tableName = ctx.GetLowercaseText(next);
        ctx.Advance();

        ctx.ExpectEof();
        if (ctx.HasErrors) return ctx.Fail();

        return ParseResult.Ok(new DescribeQuery { Table = tableName });
    }
}

namespace SproutDB.Core.Parsing;

internal static class AlterColumnParser
{
    public static ParseResult Parse(ParserContext ctx)
    {
        // Expect "column" keyword
        if (!ctx.MatchKeyword("column"))
        {
            var t = ctx.Peek();
            return ctx.Error(t, ErrorCodes.SYNTAX_ERROR, ErrorMessages.EXPECTED_COLUMN_KEYWORD);
        }

        // table.column (dot-separated)
        var tableToken = ctx.Peek();
        if (tableToken.Type != TokenType.Identifier)
            return ctx.Error(tableToken, ErrorCodes.SYNTAX_ERROR, ErrorMessages.EXPECTED_TABLE_NAME);

        var tableName = ctx.GetLowercaseText(tableToken);
        ctx.Advance();

        // Dot
        var dotToken = ctx.Peek();
        if (dotToken.Type != TokenType.Dot)
            return ctx.Error(dotToken, ErrorCodes.SYNTAX_ERROR, ErrorMessages.EXPECTED_DOT);
        ctx.Advance();

        // Column name
        var colNameToken = ctx.Peek();
        if (colNameToken.Type != TokenType.Identifier)
            return ctx.Error(colNameToken, ErrorCodes.SYNTAX_ERROR, ErrorMessages.EXPECTED_COLUMN_NAME);

        var colName = ctx.GetLowercaseText(colNameToken);
        ctx.Advance();

        if (colName == "_id")
            return ctx.Error(colNameToken, ErrorCodes.SYNTAX_ERROR, ErrorMessages.RESERVED_COLUMN_NAME_ID);

        // "string" keyword (only string columns can be altered for now)
        if (!ctx.MatchKeyword("string"))
        {
            var t = ctx.Peek();
            return ctx.Error(t, ErrorCodes.SYNTAX_ERROR, ErrorMessages.EXPECTED_STRING_KEYWORD);
        }

        // New size (required)
        var sizeToken = ctx.Peek();
        if (sizeToken.Type != TokenType.IntegerLiteral)
            return ctx.Error(sizeToken, ErrorCodes.SYNTAX_ERROR, ErrorMessages.EXPECTED_SIZE);

        var newSize = int.Parse(ctx.GetText(sizeToken));
        ctx.Advance();

        ctx.ExpectEof();
        if (ctx.HasErrors) return ctx.Fail();

        return ParseResult.Ok(new AlterColumnQuery
        {
            Table = tableName,
            Column = colName,
            NewSize = newSize,
        });
    }
}

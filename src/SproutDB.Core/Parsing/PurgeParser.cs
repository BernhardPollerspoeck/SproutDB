namespace SproutDB.Core.Parsing;

internal static class PurgeParser
{
    public static ParseResult Parse(ParserContext ctx)
    {
        var current = ctx.Peek();

        if (current.Type == TokenType.Eof)
            return ctx.Error(current, ErrorCodes.SYNTAX_ERROR, ErrorMessages.EXPECTED_PURGE_TARGET);

        if (ctx.MatchKeyword("column"))
            return ParseColumn(ctx);

        if (ctx.MatchKeyword("table"))
            return ParseTable(ctx);

        if (ctx.MatchKeyword("database"))
            return ParseDatabase(ctx);

        return ctx.Error(current, ErrorCodes.SYNTAX_ERROR, ErrorMessages.EXPECTED_PURGE_TARGET);
    }

    private static ParseResult ParseColumn(ParserContext ctx)
    {
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

        if (colName == "id")
            return ctx.Error(colNameToken, ErrorCodes.SYNTAX_ERROR, ErrorMessages.RESERVED_COLUMN_NAME_ID);

        ctx.ExpectEof();
        if (ctx.HasErrors) return ctx.Fail();

        return ParseResult.Ok(new PurgeColumnQuery
        {
            Table = tableName,
            Column = colName,
        });
    }

    private static ParseResult ParseTable(ParserContext ctx)
    {
        var tableToken = ctx.Peek();
        if (tableToken.Type != TokenType.Identifier)
            return ctx.Error(tableToken, ErrorCodes.SYNTAX_ERROR, ErrorMessages.EXPECTED_TABLE_NAME);

        var tableName = ctx.GetLowercaseText(tableToken);
        ctx.Advance();

        ctx.ExpectEof();
        if (ctx.HasErrors) return ctx.Fail();

        return ParseResult.Ok(new PurgeTableQuery { Table = tableName });
    }

    private static ParseResult ParseDatabase(ParserContext ctx)
    {
        ctx.ExpectEof();
        return ctx.HasErrors
            ? ctx.Fail()
            : ParseResult.Ok(new PurgeDatabaseQuery());
    }
}

namespace SproutDB.Core.Parsing;

internal static class CreateIndexParser
{
    public static ParseResult Parse(ParserContext ctx)
    {
        // Optional: unique keyword
        var isUnique = ctx.MatchKeyword("unique");

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

        ctx.ExpectEof();
        if (ctx.HasErrors) return ctx.Fail();

        return ParseResult.Ok(new CreateIndexQuery
        {
            Table = tableName,
            Column = colName,
            Unique = isUnique,
        });
    }
}

namespace SproutDB.Core.Parsing;

internal static class RenameColumnParser
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

        // Old column name
        var oldColToken = ctx.Peek();
        if (oldColToken.Type != TokenType.Identifier)
            return ctx.Error(oldColToken, ErrorCodes.SYNTAX_ERROR, ErrorMessages.EXPECTED_COLUMN_NAME);

        var oldColName = ctx.GetLowercaseText(oldColToken);
        ctx.Advance();

        if (oldColName == "_id")
            return ctx.Error(oldColToken, ErrorCodes.SYNTAX_ERROR, ErrorMessages.RESERVED_COLUMN_NAME_ID);

        // "to" keyword
        if (!ctx.MatchKeyword("to"))
        {
            var t = ctx.Peek();
            return ctx.Error(t, ErrorCodes.SYNTAX_ERROR, ErrorMessages.EXPECTED_TO_KEYWORD);
        }

        // New column name
        var newColToken = ctx.Peek();
        if (newColToken.Type != TokenType.Identifier)
            return ctx.Error(newColToken, ErrorCodes.SYNTAX_ERROR, ErrorMessages.EXPECTED_COLUMN_NAME);

        var newColName = ctx.GetLowercaseText(newColToken);
        ctx.Advance();

        if (newColName == "_id")
            return ctx.Error(newColToken, ErrorCodes.SYNTAX_ERROR, ErrorMessages.RESERVED_COLUMN_NAME_ID);

        ctx.ExpectEof();
        if (ctx.HasErrors) return ctx.Fail();

        return ParseResult.Ok(new RenameColumnQuery
        {
            Table = tableName,
            OldColumn = oldColName,
            NewColumn = newColName,
        });
    }
}

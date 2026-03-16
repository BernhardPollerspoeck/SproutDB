namespace SproutDB.Core.Parsing;

internal static class ShrinkParser
{
    public static ParseResult Parse(ParserContext ctx)
    {
        if (ctx.MatchKeyword("table"))
            return ParseShrinkTable(ctx);

        if (ctx.MatchKeyword("database"))
            return ParseShrinkDatabase(ctx);

        return ctx.Error(ctx.Peek(), ErrorCodes.SYNTAX_ERROR, "expected 'table' or 'database' after 'shrink'");
    }

    private static ParseResult ParseShrinkTable(ParserContext ctx)
    {
        var nameToken = ctx.Peek();
        if (nameToken.Type != TokenType.Identifier)
            return ctx.Error(nameToken, ErrorCodes.SYNTAX_ERROR, ErrorMessages.EXPECTED_TABLE_NAME);

        var tableName = ctx.GetLowercaseText(nameToken);
        ctx.Advance();

        int chunkSize = ParseOptionalChunkSize(ctx);
        if (ctx.HasErrors) return ctx.Fail();

        ctx.ExpectEof();
        if (ctx.HasErrors) return ctx.Fail();

        return ParseResult.Ok(new ShrinkTableQuery { Table = tableName, ChunkSize = chunkSize });
    }

    private static ParseResult ParseShrinkDatabase(ParserContext ctx)
    {
        int chunkSize = ParseOptionalChunkSize(ctx);
        if (ctx.HasErrors) return ctx.Fail();

        ctx.ExpectEof();
        if (ctx.HasErrors) return ctx.Fail();

        return ParseResult.Ok(new ShrinkDatabaseQuery { ChunkSize = chunkSize });
    }

    private static int ParseOptionalChunkSize(ParserContext ctx)
    {
        if (!ctx.MatchKeyword("chunk_size"))
            return 0;

        var sizeToken = ctx.Peek();
        if (sizeToken.Type != TokenType.IntegerLiteral)
        {
            ctx.AddError(sizeToken, ErrorCodes.SYNTAX_ERROR, "expected integer after 'chunk_size'");
            return 0;
        }

        var chunkSize = int.Parse(ctx.GetText(sizeToken));
        ctx.Advance();

        if (chunkSize < 100 || chunkSize > 1_000_000)
        {
            ctx.AddError(sizeToken, ErrorCodes.SYNTAX_ERROR, "chunk_size must be between 100 and 1000000");
            return 0;
        }

        return chunkSize;
    }
}

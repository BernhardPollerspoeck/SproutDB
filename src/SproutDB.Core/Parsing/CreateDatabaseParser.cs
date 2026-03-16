namespace SproutDB.Core.Parsing;

internal static class CreateDatabaseParser
{
    public static ParseResult Parse(ParserContext ctx)
    {
        int chunkSize = 0;

        if (ctx.MatchKeyword("with"))
        {
            if (!ctx.MatchKeyword("chunk_size"))
                return ctx.Error(ctx.Peek(), ErrorCodes.SYNTAX_ERROR, "expected 'chunk_size' after 'with'");

            var sizeToken = ctx.Peek();
            if (sizeToken.Type != TokenType.IntegerLiteral)
                return ctx.Error(sizeToken, ErrorCodes.SYNTAX_ERROR, "expected integer after 'chunk_size'");

            chunkSize = int.Parse(ctx.GetText(sizeToken));
            ctx.Advance();

            if (chunkSize < 100 || chunkSize > 1_000_000)
                return ctx.Error(sizeToken, ErrorCodes.SYNTAX_ERROR, "chunk_size must be between 100 and 1000000");
        }

        ctx.ExpectEof();
        if (ctx.HasErrors) return ctx.Fail();

        return ParseResult.Ok(new CreateDatabaseQuery { ChunkSize = chunkSize });
    }
}

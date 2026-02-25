namespace SproutDB.Core.Parsing;

internal static class QueryParser
{
    public static ParseResult Parse(string input)
    {
        var tokens = Tokenizer.Tokenize(input);
        var ctx = new ParserContext(input, tokens);
        return ParseQuery(ctx);
    }

    // ── Top-level dispatch ───────────────────────────────────

    private static ParseResult ParseQuery(ParserContext ctx)
    {
        var current = ctx.Peek();

        if (current.Type == TokenType.Eof)
            return ctx.Error(current, ErrorCodes.SYNTAX_ERROR, ErrorMessages.EXPECTED_COMMAND);

        if (current.Type != TokenType.Identifier)
            return ctx.Error(current, ErrorCodes.SYNTAX_ERROR, ErrorMessages.EXPECTED_COMMAND);

        if (ctx.MatchKeyword("create"))
            return ParseCreate(ctx);

        if (ctx.MatchKeyword("get"))
            return GetParser.Parse(ctx);

        if (ctx.MatchKeyword("upsert"))
            return UpsertParser.Parse(ctx);

        if (ctx.MatchKeyword("add"))
            return AddColumnParser.Parse(ctx);

        return ctx.Error(current, ErrorCodes.SYNTAX_ERROR, ErrorMessages.UNKNOWN_COMMAND);
    }

    private static ParseResult ParseCreate(ParserContext ctx)
    {
        var current = ctx.Peek();

        if (current.Type == TokenType.Eof)
            return ctx.Error(current, ErrorCodes.SYNTAX_ERROR, ErrorMessages.EXPECTED_CREATE_TARGET);

        if (ctx.MatchKeyword("database"))
            return CreateDatabaseParser.Parse(ctx);

        if (ctx.MatchKeyword("table"))
            return CreateTableParser.Parse(ctx);

        return ctx.Error(current, ErrorCodes.SYNTAX_ERROR, ErrorMessages.EXPECTED_CREATE_TARGET);
    }
}

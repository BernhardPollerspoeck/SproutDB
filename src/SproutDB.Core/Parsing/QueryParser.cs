namespace SproutDB.Core.Parsing;

internal static class QueryParser
{
    public static ParseResult Parse(string input)
    {
        var tokens = Tokenizer.Tokenize(input);
        var ctx = new ParserContext(input, tokens);
        return ParseQuery(ctx);
    }

    /// <summary>
    /// Parses a multi-query input separated by semicolons.
    /// Handles atomic...commit transaction blocks.
    /// </summary>
    public static List<ParseResult> ParseMulti(string input)
    {
        var tokens = Tokenizer.Tokenize(input);
        var results = new List<ParseResult>();

        // Split tokens into segments at semicolons
        var segments = SplitAtSemicolons(tokens);

        var i = 0;
        while (i < segments.Count)
        {
            var segment = segments[i];

            // Skip empty segments (e.g. "get users;;get orders")
            if (segment.Count == 1 && segment[0].Type == TokenType.Eof)
            {
                i++;
                continue;
            }

            // Check if this segment starts with "atomic"
            if (segment.Count > 0 && segment[0].Type == TokenType.Identifier
                && input.AsSpan(segment[0].Start, segment[0].Length).Equals("atomic", StringComparison.OrdinalIgnoreCase))
            {
                var txResult = ParseTransactionBlock(input, segments, ref i);
                results.Add(txResult);
                continue;
            }

            var ctx = new ParserContext(input, segment);
            var result = ParseQuery(ctx);
            result.OriginalText = ExtractSegmentText(input, segment);
            results.Add(result);
            i++;
        }

        // If no segments produced results (e.g. input was just ";;" or empty),
        // return a single error
        if (results.Count == 0)
        {
            var ctx = new ParserContext(input, tokens);
            results.Add(ctx.Error(tokens[0], ErrorCodes.SYNTAX_ERROR, ErrorMessages.EXPECTED_COMMAND));
        }

        return results;
    }

    /// <summary>
    /// Splits the token list into segments at Semicolon tokens.
    /// Each segment ends with an Eof token.
    /// </summary>
    private static List<List<Token>> SplitAtSemicolons(List<Token> tokens)
    {
        var segments = new List<List<Token>>();
        var current = new List<Token>();

        foreach (var token in tokens)
        {
            if (token.Type == TokenType.Semicolon)
            {
                // End current segment with Eof
                current.Add(new Token(TokenType.Eof, token.Start, 0));
                segments.Add(current);
                current = new List<Token>();
                continue;
            }

            current.Add(token);
        }

        // Last segment (already has Eof from tokenizer)
        if (current.Count > 0)
            segments.Add(current);

        return segments;
    }

    /// <summary>
    /// Parses an atomic...commit transaction block starting at segments[i].
    /// Advances i past the commit segment.
    /// </summary>
    private static ParseResult ParseTransactionBlock(string input, List<List<Token>> segments, ref int i)
    {
        var atomicSegment = segments[i];
        var atomicToken = atomicSegment[0];

        // Check for nested atomic (the atomic keyword itself)
        // The segment should be just "atomic" + Eof
        if (atomicSegment.Count > 2 || (atomicSegment.Count == 2 && atomicSegment[1].Type != TokenType.Eof))
        {
            // "atomic" should be alone in its segment
            var ctx = new ParserContext(input, atomicSegment);
            ctx.Advance(); // skip 'atomic'
            return ctx.Error(ctx.Peek(), ErrorCodes.SYNTAX_ERROR, "unexpected token after 'atomic'");
        }

        i++; // move past 'atomic' segment

        var innerQueries = new List<IQuery>();
        var queryTexts = new List<string>();
        var foundCommit = false;

        while (i < segments.Count)
        {
            var segment = segments[i];

            // Skip empty segments
            if (segment.Count == 1 && segment[0].Type == TokenType.Eof)
            {
                i++;
                continue;
            }

            // Check for "commit"
            if (segment.Count >= 1 && segment[0].Type == TokenType.Identifier
                && input.AsSpan(segment[0].Start, segment[0].Length).Equals("commit", StringComparison.OrdinalIgnoreCase))
            {
                foundCommit = true;
                i++;
                break;
            }

            // Check for nested "atomic"
            if (segment[0].Type == TokenType.Identifier
                && input.AsSpan(segment[0].Start, segment[0].Length).Equals("atomic", StringComparison.OrdinalIgnoreCase))
            {
                SkipToCommit(input, segments, ref i);
                var ctx = new ParserContext(input, segment);
                return ctx.Error(segment[0], ErrorCodes.SYNTAX_ERROR, "nested transactions are not allowed");
            }

            // Parse inner query
            var innerCtx = new ParserContext(input, segment);
            var innerResult = ParseQuery(innerCtx);

            if (!innerResult.Success)
            {
                SkipToCommit(input, segments, ref i);
                return innerResult;
            }

            var innerQuery = innerResult.Query;
            if (innerQuery is null)
            {
                SkipToCommit(input, segments, ref i);
                return innerResult;
            }

            innerQueries.Add(innerQuery);
            queryTexts.Add(ExtractSegmentText(input, segment));
            i++;
        }

        if (!foundCommit)
        {
            var ctx = new ParserContext(input, [new Token(TokenType.Eof, input.Length, 0)]);
            return ctx.Error(ctx.Peek(), ErrorCodes.SYNTAX_ERROR, "'atomic' without 'commit'");
        }

        return ParseResult.Ok(new TransactionQuery
        {
            Queries = innerQueries,
            QueryTexts = queryTexts,
        });
    }

    /// <summary>
    /// Advances i past the next 'commit' segment (or to the end).
    /// Used to skip remaining transaction segments on error.
    /// </summary>
    private static void SkipToCommit(string input, List<List<Token>> segments, ref int i)
    {
        while (i < segments.Count)
        {
            var seg = segments[i];
            i++;
            if (seg.Count >= 1 && seg[0].Type == TokenType.Identifier
                && input.AsSpan(seg[0].Start, seg[0].Length).Equals("commit", StringComparison.OrdinalIgnoreCase))
            {
                break;
            }
        }
    }

    /// <summary>
    /// Extracts the original text for a token segment from the full input.
    /// </summary>
    private static string ExtractSegmentText(string input, List<Token> segment)
    {
        if (segment.Count == 0)
            return string.Empty;

        var first = segment[0];
        // Find the last non-Eof token
        var last = first;
        for (var j = segment.Count - 1; j >= 0; j--)
        {
            if (segment[j].Type != TokenType.Eof)
            {
                last = segment[j];
                break;
            }
        }

        var start = first.Start;
        var end = last.Start + last.Length;
        if (end <= start)
            return input.Substring(start, first.Length);

        return input[start..end].Trim();
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

        if (ctx.MatchKeyword("describe"))
            return DescribeParser.Parse(ctx);

        if (ctx.MatchKeyword("upsert"))
            return UpsertParser.Parse(ctx);

        if (ctx.MatchKeyword("add"))
            return AddColumnParser.Parse(ctx);

        if (ctx.MatchKeyword("purge"))
            return PurgeParser.Parse(ctx);

        if (ctx.MatchKeyword("rename"))
            return RenameColumnParser.Parse(ctx);

        if (ctx.MatchKeyword("alter"))
            return AlterColumnParser.Parse(ctx);

        if (ctx.MatchKeyword("backup"))
            return BackupParser.Parse(ctx);

        if (ctx.MatchKeyword("restore"))
            return RestoreParser.Parse(ctx);

        if (ctx.MatchKeyword("delete"))
            return DeleteParser.Parse(ctx);

        if (ctx.MatchKeyword("grant"))
            return GrantParser.Parse(ctx);

        if (ctx.MatchKeyword("revoke"))
            return RevokeParser.Parse(ctx);

        if (ctx.MatchKeyword("restrict"))
            return RestrictParser.Parse(ctx);

        if (ctx.MatchKeyword("unrestrict"))
            return UnrestrictParser.Parse(ctx);

        if (ctx.MatchKeyword("rotate"))
            return ParseRotate(ctx);

        if (ctx.MatchKeyword("shrink"))
            return ShrinkParser.Parse(ctx);

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

        if (ctx.MatchKeyword("index"))
            return CreateIndexParser.Parse(ctx);

        if (ctx.MatchKeyword("apikey"))
            return CreateApiKeyParser.Parse(ctx);

        return ctx.Error(current, ErrorCodes.SYNTAX_ERROR, ErrorMessages.EXPECTED_CREATE_TARGET);
    }

    private static ParseResult ParseRotate(ParserContext ctx)
    {
        var current = ctx.Peek();

        if (current.Type == TokenType.Eof)
            return ctx.Error(current, ErrorCodes.SYNTAX_ERROR, "expected 'apikey'");

        if (ctx.MatchKeyword("apikey"))
            return RotateApiKeyParser.Parse(ctx);

        return ctx.Error(current, ErrorCodes.SYNTAX_ERROR, "expected 'apikey'");
    }
}

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

        // Optional: select / -select
        List<SelectColumn>? selectColumns = null;
        var excludeSelect = false;

        if (ctx.Peek().Type != TokenType.Eof)
        {
            if (ctx.MatchKeyword("select"))
            {
                selectColumns = ParseSelectList(ctx);
                if (ctx.HasErrors) return ctx.Fail();
            }
            else if (ctx.Peek().Type == TokenType.Minus)
            {
                // Peek ahead: minus followed by 'select' keyword?
                var minusToken = ctx.Peek();
                ctx.Advance();
                if (ctx.MatchKeyword("select"))
                {
                    excludeSelect = true;
                    selectColumns = ParseSelectList(ctx);
                    if (ctx.HasErrors) return ctx.Fail();
                }
                else
                {
                    // Not -select, error
                    return ctx.Error(minusToken, ErrorCodes.SYNTAX_ERROR,
                        "expected 'select' after '-'");
                }
            }
        }

        // Optional: where <column> <op> <value>
        WhereClause? where = null;

        if (ctx.Peek().Type != TokenType.Eof && ctx.MatchKeyword("where"))
        {
            where = ParseWhere(ctx);
            if (ctx.HasErrors) return ctx.Fail();
        }

        ctx.ExpectEof();
        if (ctx.HasErrors) return ctx.Fail();

        return ParseResult.Ok(new GetQuery
        {
            Table = tableName,
            Select = selectColumns,
            ExcludeSelect = excludeSelect,
            Where = where,
        });
    }

    // ── Where ─────────────────────────────────────────────────

    private static WhereClause? ParseWhere(ParserContext ctx)
    {
        // Column name
        var colToken = ctx.Peek();
        if (colToken.Type != TokenType.Identifier)
        {
            ctx.AddError(colToken, ErrorCodes.SYNTAX_ERROR, ErrorMessages.EXPECTED_COLUMN_NAME);
            return null;
        }
        var colName = ctx.GetLowercaseText(colToken);
        ctx.Advance();

        // Operator
        var opToken = ctx.Peek();
        var op = opToken.Type switch
        {
            TokenType.Equals => CompareOp.Equal,
            TokenType.NotEqual => CompareOp.NotEqual,
            TokenType.GreaterThan => CompareOp.GreaterThan,
            TokenType.GreaterThanOrEqual => CompareOp.GreaterThanOrEqual,
            TokenType.LessThan => CompareOp.LessThan,
            TokenType.LessThanOrEqual => CompareOp.LessThanOrEqual,
            _ => (CompareOp?)null,
        };

        if (op is null)
        {
            ctx.AddError(opToken, ErrorCodes.SYNTAX_ERROR, "expected comparison operator ('=', '!=', '>', '>=', '<', '<=')");
            return null;
        }
        ctx.Advance();

        // Value
        var valueToken = ctx.Peek();
        string? value = ParseLiteralValue(ctx, valueToken);
        if (value is null)
        {
            ctx.AddError(valueToken, ErrorCodes.SYNTAX_ERROR, ErrorMessages.EXPECTED_VALUE);
            return null;
        }

        return new WhereClause
        {
            Column = colName,
            ColumnPosition = colToken.Start,
            ColumnLength = colToken.Length,
            Operator = op.Value,
            Value = value,
        };
    }

    /// <summary>
    /// Parses a literal value (string, integer, float, boolean) and returns
    /// the raw string representation. Advances the context past the value tokens.
    /// Returns null if the current token is not a valid literal.
    /// </summary>
    internal static string? ParseLiteralValue(ParserContext ctx, Token token)
    {
        switch (token.Type)
        {
            case TokenType.StringLiteral:
                ctx.Advance();
                // Strip quotes
                return ctx.Input.Substring(token.Start + 1, token.Length - 2);

            case TokenType.IntegerLiteral:
                ctx.Advance();
                return ctx.GetText(token);

            case TokenType.FloatLiteral:
                ctx.Advance();
                return ctx.GetText(token);

            case TokenType.Minus:
            {
                // Negative number
                ctx.Advance();
                var numToken = ctx.Peek();
                if (numToken.Type is TokenType.IntegerLiteral or TokenType.FloatLiteral)
                {
                    ctx.Advance();
                    return "-" + ctx.GetText(numToken);
                }
                return null;
            }

            case TokenType.Identifier:
                if (ctx.IsKeyword(token, "true") || ctx.IsKeyword(token, "false"))
                {
                    ctx.Advance();
                    return ctx.GetLowercaseText(token);
                }
                return null;

            default:
                return null;
        }
    }

    // ── Select ────────────────────────────────────────────────

    private static List<SelectColumn> ParseSelectList(ParserContext ctx)
    {
        var columns = new List<SelectColumn>();

        while (true)
        {
            var token = ctx.Peek();
            if (token.Type != TokenType.Identifier)
            {
                ctx.AddError(token, ErrorCodes.SYNTAX_ERROR, ErrorMessages.EXPECTED_COLUMN_NAME);
                return columns;
            }

            columns.Add(new SelectColumn(ctx.GetLowercaseText(token), token.Start, token.Length));
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

namespace SproutDB.Core.Parsing;

internal static class AddColumnParser
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

        // Column type
        var typeToken = ctx.Peek();
        if (!ctx.TryMatchColumnType(typeToken))
            return ctx.Error(typeToken, ErrorCodes.SYNTAX_ERROR, ErrorMessages.EXPECTED_COLUMN_TYPE);

        ColumnTypes.TryParse(ctx.Input.AsSpan(typeToken.Start, typeToken.Length), out var colType);
        ctx.Advance();

        // Optional string size
        var size = ColumnTypes.GetDefaultSize(colType);
        if (colType == ColumnType.String && ctx.Peek().Type == TokenType.IntegerLiteral)
        {
            size = int.Parse(ctx.GetText(ctx.Peek()));
            ctx.Advance();
        }

        // Optional modifiers: strict / default (any order)
        var strict = false;
        string? defaultValue = null;

        for (var i = 0; i < 2; i++)
        {
            if (ctx.MatchKeyword("strict"))
            {
                strict = true;
            }
            else if (ctx.MatchKeyword("default"))
            {
                defaultValue = ParseDefaultValue(ctx);
                if (ctx.HasErrors) return ctx.Fail();
            }
            else
            {
                break;
            }
        }

        ctx.ExpectEof();
        if (ctx.HasErrors) return ctx.Fail();

        return ParseResult.Ok(new AddColumnQuery
        {
            Table = tableName,
            Column = new ColumnDefinition
            {
                Name = colName,
                Type = colType,
                Size = size,
                Strict = strict,
                Default = defaultValue,
            },
        });
    }

    private static string ParseDefaultValue(ParserContext ctx)
    {
        var token = ctx.Peek();

        if (token.Type == TokenType.Minus)
        {
            ctx.Advance();
            var numToken = ctx.Peek();
            if (numToken.Type is TokenType.IntegerLiteral or TokenType.FloatLiteral)
            {
                ctx.Advance();
                return "-" + ctx.GetText(numToken);
            }
            ctx.AddError(numToken, ErrorCodes.SYNTAX_ERROR, ErrorMessages.EXPECTED_DEFAULT_VALUE);
            return null!;
        }

        if (token.Type is TokenType.IntegerLiteral or TokenType.FloatLiteral)
        {
            ctx.Advance();
            return ctx.GetText(token);
        }

        if (token.Type == TokenType.StringLiteral)
        {
            ctx.Advance();
            return ctx.Input.Substring(token.Start + 1, token.Length - 2);
        }

        if (token.Type == TokenType.Identifier)
        {
            if (ctx.IsKeyword(token, "true") || ctx.IsKeyword(token, "false"))
            {
                ctx.Advance();
                return ctx.GetLowercaseText(token);
            }
        }

        ctx.AddError(token, ErrorCodes.SYNTAX_ERROR, ErrorMessages.EXPECTED_DEFAULT_VALUE);
        return null!;
    }
}

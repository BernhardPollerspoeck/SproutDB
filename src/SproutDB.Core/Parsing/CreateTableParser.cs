namespace SproutDB.Core.Parsing;

internal static class CreateTableParser
{
    public static ParseResult Parse(ParserContext ctx)
    {
        // Table name
        var nameToken = ctx.Peek();
        if (nameToken.Type != TokenType.Identifier || ctx.TryMatchColumnType(nameToken))
            return ctx.Error(nameToken, ErrorCodes.SYNTAX_ERROR, ErrorMessages.EXPECTED_TABLE_NAME);

        var tableName = ctx.GetLowercaseText(nameToken);
        ctx.Advance();

        if (!SproutEngine.IsValidName(tableName))
            return ctx.Error(nameToken, ErrorCodes.SYNTAX_ERROR, $"invalid table name '{tableName}'");

        // Optional column definitions
        var columns = new List<ColumnDefinition>();
        if (ctx.Peek().Type == TokenType.LeftParen)
        {
            ctx.Advance();
            columns = ParseColumnDefinitions(ctx);
            if (ctx.HasErrors) return ctx.Fail();
        }

        ctx.ExpectEof();
        if (ctx.HasErrors) return ctx.Fail();

        return ParseResult.Ok(new CreateTableQuery
        {
            Table = tableName,
            Columns = columns,
        });
    }

    private static List<ColumnDefinition> ParseColumnDefinitions(ParserContext ctx)
    {
        var columns = new List<ColumnDefinition>();

        while (true)
        {
            var col = ParseColumnDefinition(ctx);
            if (ctx.HasErrors) return columns;
            columns.Add(col);

            if (ctx.Peek().Type == TokenType.Comma)
            {
                ctx.Advance();
                continue;
            }

            if (ctx.Peek().Type == TokenType.RightParen)
            {
                ctx.Advance();
                break;
            }

            ctx.AddError(ctx.Peek(), ErrorCodes.SYNTAX_ERROR, ErrorMessages.EXPECTED_COMMA_OR_CLOSE_PAREN);
            return columns;
        }

        return columns;
    }

    private static ColumnDefinition ParseColumnDefinition(ParserContext ctx)
    {
        // Column name
        var nameToken = ctx.Peek();
        if (nameToken.Type != TokenType.Identifier)
        {
            ctx.AddError(nameToken, ErrorCodes.SYNTAX_ERROR, ErrorMessages.EXPECTED_COLUMN_NAME);
            return null!;
        }
        var name = ctx.GetLowercaseText(nameToken);
        ctx.Advance();

        if (name == "id")
        {
            ctx.AddError(nameToken, ErrorCodes.SYNTAX_ERROR, ErrorMessages.RESERVED_COLUMN_NAME_ID);
            return null!;
        }

        // Column type
        var typeToken = ctx.Peek();
        if (!ctx.TryMatchColumnType(typeToken))
        {
            ctx.AddError(typeToken, ErrorCodes.SYNTAX_ERROR, ErrorMessages.EXPECTED_COLUMN_TYPE);
            return null!;
        }
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
                if (ctx.HasErrors) return null!;
            }
            else
            {
                break;
            }
        }

        return new ColumnDefinition
        {
            Name = name,
            Type = colType,
            Size = size,
            Strict = strict,
            Default = defaultValue,
        };
    }

    private static string ParseDefaultValue(ParserContext ctx)
    {
        var token = ctx.Peek();

        // Negative number
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

        // Number
        if (token.Type is TokenType.IntegerLiteral or TokenType.FloatLiteral)
        {
            ctx.Advance();
            return ctx.GetText(token);
        }

        // String literal (strip quotes)
        if (token.Type == TokenType.StringLiteral)
        {
            ctx.Advance();
            return ctx.Input.Substring(token.Start + 1, token.Length - 2);
        }

        // Boolean (true/false)
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

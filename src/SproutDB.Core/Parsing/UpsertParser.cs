namespace SproutDB.Core.Parsing;

internal static class UpsertParser
{
    public static ParseResult Parse(ParserContext ctx)
    {
        // Table name
        var nameToken = ctx.Peek();
        if (nameToken.Type != TokenType.Identifier)
            return ctx.Error(nameToken, ErrorCodes.SYNTAX_ERROR, ErrorMessages.EXPECTED_TABLE_NAME);

        var tableName = ctx.GetLowercaseText(nameToken);
        ctx.Advance();

        // Opening brace
        var braceToken = ctx.Peek();
        if (braceToken.Type != TokenType.LeftBrace)
            return ctx.Error(braceToken, ErrorCodes.SYNTAX_ERROR, ErrorMessages.EXPECTED_OPEN_BRACE);
        ctx.Advance();

        // Fields
        var fields = ParseFields(ctx);
        if (ctx.HasErrors) return ctx.Fail();

        ctx.ExpectEof();
        if (ctx.HasErrors) return ctx.Fail();

        return ParseResult.Ok(new UpsertQuery
        {
            Table = tableName,
            Fields = fields,
        });
    }

    private static List<UpsertField> ParseFields(ParserContext ctx)
    {
        var fields = new List<UpsertField>();

        // Empty object {}
        if (ctx.Peek().Type == TokenType.RightBrace)
        {
            ctx.Advance();
            return fields;
        }

        while (true)
        {
            var field = ParseField(ctx);
            if (ctx.HasErrors) return fields;
            fields.Add(field);

            if (ctx.Peek().Type == TokenType.Comma)
            {
                ctx.Advance();
                continue;
            }

            if (ctx.Peek().Type == TokenType.RightBrace)
            {
                ctx.Advance();
                break;
            }

            ctx.AddError(ctx.Peek(), ErrorCodes.SYNTAX_ERROR, ErrorMessages.EXPECTED_COMMA_OR_CLOSE_BRACE);
            return fields;
        }

        return fields;
    }

    private static UpsertField ParseField(ParserContext ctx)
    {
        // Field name
        var nameToken = ctx.Peek();
        if (nameToken.Type != TokenType.Identifier)
        {
            ctx.AddError(nameToken, ErrorCodes.SYNTAX_ERROR, ErrorMessages.EXPECTED_FIELD_NAME);
            return null!;
        }
        var name = ctx.GetLowercaseText(nameToken);
        ctx.Advance();

        // Colon
        var colonToken = ctx.Peek();
        if (colonToken.Type != TokenType.Colon)
        {
            ctx.AddError(colonToken, ErrorCodes.SYNTAX_ERROR, ErrorMessages.EXPECTED_COLON);
            return null!;
        }
        ctx.Advance();

        // Value
        var value = ParseValue(ctx);
        if (ctx.HasErrors) return null!;

        return new UpsertField
        {
            Name = name,
            Value = value,
        };
    }

    private static UpsertValue ParseValue(ParserContext ctx)
    {
        var token = ctx.Peek();

        // null
        if (token.Type == TokenType.Identifier && ctx.IsKeyword(token, "null"))
        {
            ctx.Advance();
            return new UpsertValue { Kind = UpsertValueKind.Null };
        }

        // true / false
        if (token.Type == TokenType.Identifier &&
            (ctx.IsKeyword(token, "true") || ctx.IsKeyword(token, "false")))
        {
            ctx.Advance();
            return new UpsertValue
            {
                Kind = UpsertValueKind.Boolean,
                Raw = ctx.GetLowercaseText(token),
            };
        }

        // String literal
        if (token.Type == TokenType.StringLiteral)
        {
            ctx.Advance();
            return new UpsertValue
            {
                Kind = UpsertValueKind.String,
                Raw = ctx.Input.Substring(token.Start + 1, token.Length - 2),
            };
        }

        // Negative number
        if (token.Type == TokenType.Minus)
        {
            ctx.Advance();
            var numToken = ctx.Peek();
            if (numToken.Type == TokenType.IntegerLiteral)
            {
                ctx.Advance();
                return new UpsertValue
                {
                    Kind = UpsertValueKind.Integer,
                    Raw = "-" + ctx.GetText(numToken),
                };
            }
            if (numToken.Type == TokenType.FloatLiteral)
            {
                ctx.Advance();
                return new UpsertValue
                {
                    Kind = UpsertValueKind.Float,
                    Raw = "-" + ctx.GetText(numToken),
                };
            }
            ctx.AddError(numToken, ErrorCodes.SYNTAX_ERROR, ErrorMessages.EXPECTED_VALUE);
            return null!;
        }

        // Integer
        if (token.Type == TokenType.IntegerLiteral)
        {
            ctx.Advance();
            return new UpsertValue
            {
                Kind = UpsertValueKind.Integer,
                Raw = ctx.GetText(token),
            };
        }

        // Float
        if (token.Type == TokenType.FloatLiteral)
        {
            ctx.Advance();
            return new UpsertValue
            {
                Kind = UpsertValueKind.Float,
                Raw = ctx.GetText(token),
            };
        }

        ctx.AddError(token, ErrorCodes.SYNTAX_ERROR, ErrorMessages.EXPECTED_VALUE);
        return null!;
    }
}

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

        // Single {…} or bulk [{…}, {…}]
        List<List<UpsertField>> records;
        List<long> rowTtlSeconds;
        var next = ctx.Peek();

        if (next.Type == TokenType.LeftBracket)
        {
            ctx.Advance();
            (records, rowTtlSeconds) = ParseBulkRecordsWithTtl(ctx);
            if (ctx.HasErrors) return ctx.Fail();
        }
        else if (next.Type == TokenType.LeftBrace)
        {
            ctx.Advance();
            var (fields, ttl) = ParseFieldsWithTtl(ctx);
            if (ctx.HasErrors) return ctx.Fail();
            records = [fields];
            rowTtlSeconds = [ttl];
        }
        else
        {
            return ctx.Error(next, ErrorCodes.SYNTAX_ERROR, ErrorMessages.EXPECTED_OPEN_BRACE);
        }

        // Optional ON clause
        string? onColumn = null;
        if (ctx.Peek().Type == TokenType.Identifier && ctx.IsKeyword(ctx.Peek(), "on"))
        {
            ctx.Advance();
            var colToken = ctx.Peek();
            if (colToken.Type != TokenType.Identifier)
                return ctx.Error(colToken, ErrorCodes.SYNTAX_ERROR, "expected column name after 'on'");
            onColumn = ctx.GetLowercaseText(colToken);
            ctx.Advance();
        }

        ctx.ExpectEof();
        if (ctx.HasErrors) return ctx.Fail();

        return ParseResult.Ok(new UpsertQuery
        {
            Table = tableName,
            Records = records,
            OnColumn = onColumn,
            RowTtlSeconds = rowTtlSeconds,
        });
    }

    private static (List<List<UpsertField>>, List<long>) ParseBulkRecordsWithTtl(ParserContext ctx)
    {
        var records = new List<List<UpsertField>>();
        var ttls = new List<long>();

        while (true)
        {
            var token = ctx.Peek();
            if (token.Type != TokenType.LeftBrace)
            {
                ctx.AddError(token, ErrorCodes.SYNTAX_ERROR, ErrorMessages.EXPECTED_OPEN_BRACE);
                return (records, ttls);
            }
            ctx.Advance();

            var (fields, ttl) = ParseFieldsWithTtl(ctx);
            if (ctx.HasErrors) return (records, ttls);
            records.Add(fields);
            ttls.Add(ttl);

            if (ctx.Peek().Type == TokenType.Comma)
            {
                ctx.Advance();
                continue;
            }

            if (ctx.Peek().Type == TokenType.RightBracket)
            {
                ctx.Advance();
                break;
            }

            ctx.AddError(ctx.Peek(), ErrorCodes.SYNTAX_ERROR, "expected ',' or ']'");
            return (records, ttls);
        }

        return (records, ttls);
    }

    private static (List<UpsertField>, long) ParseFieldsWithTtl(ParserContext ctx)
    {
        var fields = new List<UpsertField>();
        long ttlSeconds = 0;

        // Empty object {}
        if (ctx.Peek().Type == TokenType.RightBrace)
        {
            ctx.Advance();
            return (fields, 0);
        }

        while (true)
        {
            // Check if this field is 'ttl' — special handling
            var peekToken = ctx.Peek();
            if (peekToken.Type == TokenType.Identifier && ctx.IsKeyword(peekToken, "ttl"))
            {
                ctx.Advance(); // consume 'ttl'

                // Colon
                var colonToken = ctx.Peek();
                if (colonToken.Type != TokenType.Colon)
                {
                    ctx.AddError(colonToken, ErrorCodes.SYNTAX_ERROR, ErrorMessages.EXPECTED_COLON);
                    return (fields, 0);
                }
                ctx.Advance();

                // ttl: 0 means no row TTL
                var valToken = ctx.Peek();
                if (valToken.Type == TokenType.IntegerLiteral)
                {
                    var nextAfterNum = ctx.PeekAt(1);
                    if (nextAfterNum.Type == TokenType.Identifier)
                    {
                        // Duration like 7d, 24h, 30m
                        ttlSeconds = TtlDuration.ParseFromTokens(ctx);
                        if (ctx.HasErrors) return (fields, 0);
                    }
                    else
                    {
                        // Plain integer (ttl: 0)
                        ttlSeconds = long.Parse(ctx.GetText(valToken));
                        ctx.Advance();
                    }
                }
                else
                {
                    ctx.AddError(valToken, ErrorCodes.SYNTAX_ERROR, "expected TTL duration (e.g. 7d, 24h, 30m) or 0");
                    return (fields, 0);
                }
            }
            else
            {
                var field = ParseField(ctx);
                if (ctx.HasErrors) return (fields, 0);
                fields.Add(field);
            }

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
            return (fields, 0);
        }

        return (fields, ttlSeconds);
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
            Position = nameToken.Start,
            Length = nameToken.Length,
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
                Raw = ctx.Input.Substring(token.Start + 1, token.Length - 2).Replace("\\'", "'"),
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

        // Array literal: ['a', 'b', ...]
        if (token.Type == TokenType.LeftBracket)
        {
            ctx.Advance();
            var elements = new List<string>();
            var elementKind = UpsertValueKind.Null; // track element type

            if (ctx.Peek().Type != TokenType.RightBracket)
            {
                while (true)
                {
                    var elemValue = ParseValue(ctx);
                    if (ctx.HasErrors) return null!;

                    if (elemValue.Kind == UpsertValueKind.Null)
                    {
                        elements.Add("null");
                    }
                    else
                    {
                        if (elementKind == UpsertValueKind.Null)
                            elementKind = elemValue.Kind;
                        elements.Add(elemValue.Kind == UpsertValueKind.String
                            ? System.Text.Json.JsonSerializer.Serialize(elemValue.Raw)
                            : elemValue.Raw ?? "null");
                    }

                    if (ctx.Peek().Type == TokenType.Comma)
                    {
                        ctx.Advance();
                        continue;
                    }
                    break;
                }
            }

            if (ctx.Peek().Type != TokenType.RightBracket)
            {
                ctx.AddError(ctx.Peek(), ErrorCodes.SYNTAX_ERROR, "expected ']' to close array");
                return null!;
            }
            ctx.Advance();

            var json = "[" + string.Join(",", elements) + "]";
            return new UpsertValue
            {
                Kind = UpsertValueKind.Array,
                Raw = json,
            };
        }

        ctx.AddError(token, ErrorCodes.SYNTAX_ERROR, ErrorMessages.EXPECTED_VALUE);
        return null!;
    }
}

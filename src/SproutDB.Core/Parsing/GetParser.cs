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

        // Optional: aggregate (sum/avg/min/max column [as alias])
        AggregateFunction? aggregate = null;
        string? aggregateColumn = null;
        int aggregateColumnPosition = 0;
        int aggregateColumnLength = 0;
        string? aggregateAlias = null;

        if (ctx.Peek().Type != TokenType.Eof)
        {
            aggregate = TryMatchAggregate(ctx);
            if (aggregate.HasValue)
            {
                var colToken = ctx.Peek();
                if (colToken.Type != TokenType.Identifier)
                    return ctx.Error(colToken, ErrorCodes.SYNTAX_ERROR, ErrorMessages.EXPECTED_COLUMN_NAME);

                aggregateColumn = ctx.GetLowercaseText(colToken);
                aggregateColumnPosition = colToken.Start;
                aggregateColumnLength = colToken.Length;
                ctx.Advance();

                // Optional: as <alias>
                if (ctx.Peek().Type != TokenType.Eof && ctx.MatchKeyword("as"))
                {
                    var aliasToken = ctx.Peek();
                    if (aliasToken.Type != TokenType.Identifier)
                        return ctx.Error(aliasToken, ErrorCodes.SYNTAX_ERROR, "expected alias name after 'as'");
                    aggregateAlias = ctx.GetLowercaseText(aliasToken);
                    ctx.Advance();
                }
            }
        }

        // Optional: select / -select (mutually exclusive with aggregate)
        List<SelectColumn>? selectColumns = null;
        List<ComputedColumn>? computedColumns = null;
        var excludeSelect = false;

        if (aggregate is null && ctx.Peek().Type != TokenType.Eof)
        {
            if (ctx.MatchKeyword("select"))
            {
                (selectColumns, computedColumns) = ParseSelectList(ctx);
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
                    (selectColumns, computedColumns) = ParseSelectList(ctx);
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

        // Optional: distinct (only after select)
        var isDistinct = false;
        var distinctToken = ctx.Peek();
        if (distinctToken.Type != TokenType.Eof && ctx.MatchKeyword("distinct"))
        {
            if (selectColumns is null)
            {
                return ctx.Error(distinctToken, ErrorCodes.SYNTAX_ERROR,
                    "'distinct' requires a 'select' clause");
            }
            isDistinct = true;
        }

        // Optional: where <column> <op> <value>
        WhereNode? where = null;

        if (ctx.Peek().Type != TokenType.Eof && ctx.MatchKeyword("where"))
        {
            where = ParseWhere(ctx);
            if (ctx.HasErrors) return ctx.Fail();
        }

        // Optional: count
        var isCount = false;
        if (ctx.Peek().Type != TokenType.Eof && ctx.MatchKeyword("count"))
        {
            isCount = true;
        }

        // Optional: group by col1, col2
        List<SelectColumn>? groupBy = null;

        if (ctx.Peek().Type != TokenType.Eof && ctx.MatchKeyword("group"))
        {
            if (!ctx.MatchKeyword("by"))
            {
                return ctx.Error(ctx.Peek(), ErrorCodes.SYNTAX_ERROR, "expected 'by' after 'group'");
            }
            groupBy = ParseGroupByList(ctx);
            if (ctx.HasErrors) return ctx.Fail();
        }

        // Optional: order by col [desc], col2 [desc]
        List<OrderByColumn>? orderBy = null;

        if (ctx.Peek().Type != TokenType.Eof && ctx.MatchKeyword("order"))
        {
            if (!ctx.MatchKeyword("by"))
            {
                return ctx.Error(ctx.Peek(), ErrorCodes.SYNTAX_ERROR, "expected 'by' after 'order'");
            }
            orderBy = ParseOrderByList(ctx);
            if (ctx.HasErrors) return ctx.Fail();
        }

        // Optional: limit N
        int? limit = null;

        if (ctx.Peek().Type != TokenType.Eof && ctx.MatchKeyword("limit"))
        {
            var limitToken = ctx.Peek();
            if (limitToken.Type != TokenType.IntegerLiteral)
            {
                return ctx.Error(limitToken, ErrorCodes.SYNTAX_ERROR, "expected integer after 'limit'");
            }
            limit = int.Parse(ctx.GetText(limitToken));
            ctx.Advance();
        }

        // Optional: page N size M
        int? page = null;
        int? size = null;

        if (ctx.Peek().Type != TokenType.Eof && ctx.MatchKeyword("page"))
        {
            var pageToken = ctx.Peek();
            if (pageToken.Type != TokenType.IntegerLiteral)
            {
                return ctx.Error(pageToken, ErrorCodes.SYNTAX_ERROR, "expected integer after 'page'");
            }
            page = int.Parse(ctx.GetText(pageToken));
            ctx.Advance();

            if (!ctx.MatchKeyword("size"))
            {
                return ctx.Error(ctx.Peek(), ErrorCodes.SYNTAX_ERROR, "expected 'size' after page number");
            }

            var sizeToken = ctx.Peek();
            if (sizeToken.Type != TokenType.IntegerLiteral)
            {
                return ctx.Error(sizeToken, ErrorCodes.SYNTAX_ERROR, "expected integer after 'size'");
            }
            size = int.Parse(ctx.GetText(sizeToken));
            ctx.Advance();
        }

        // Optional: follow (join) clauses — zero or more
        List<FollowClause>? followClauses = null;

        while (ctx.Peek().Type != TokenType.Eof && ctx.MatchKeyword("follow"))
        {
            var followClause = ParseFollowClause(ctx, tableName);
            if (ctx.HasErrors) return ctx.Fail();
            if (followClause is not null)
            {
                followClauses ??= [];
                followClauses.Add(followClause);
            }
        }

        ctx.ExpectEof();
        if (ctx.HasErrors) return ctx.Fail();

        return ParseResult.Ok(new GetQuery
        {
            Table = tableName,
            Select = selectColumns,
            ExcludeSelect = excludeSelect,
            ComputedSelect = computedColumns,
            IsDistinct = isDistinct,
            Where = where,
            OrderBy = orderBy,
            Limit = limit,
            IsCount = isCount,
            Page = page,
            Size = size,
            Aggregate = aggregate,
            AggregateColumn = aggregateColumn,
            AggregateColumnPosition = aggregateColumnPosition,
            AggregateColumnLength = aggregateColumnLength,
            AggregateAlias = aggregateAlias,
            GroupBy = groupBy,
            Follow = followClauses,
        });
    }

    private static AggregateFunction? TryMatchAggregate(ParserContext ctx)
    {
        var token = ctx.Peek();
        if (token.Type != TokenType.Identifier)
            return null;

        AggregateFunction? fn = null;
        if (ctx.IsKeyword(token, "sum")) fn = AggregateFunction.Sum;
        else if (ctx.IsKeyword(token, "avg")) fn = AggregateFunction.Avg;
        else if (ctx.IsKeyword(token, "min")) fn = AggregateFunction.Min;
        else if (ctx.IsKeyword(token, "max")) fn = AggregateFunction.Max;

        if (fn.HasValue)
        {
            // Only consume if next token is an identifier (column name) — otherwise
            // "sum" etc. could be a table name like "get summary"
            var next = ctx.PeekAt(1);
            if (next.Type == TokenType.Identifier && !ctx.IsKeyword(next, "where")
                && !ctx.IsKeyword(next, "order") && !ctx.IsKeyword(next, "limit")
                && !ctx.IsKeyword(next, "count") && !ctx.IsKeyword(next, "group")
                && !ctx.IsKeyword(next, "page") && !ctx.IsKeyword(next, "follow"))
            {
                ctx.Advance();
                return fn;
            }
        }

        return null;
    }

    // ── Where (recursive-descent: OR < AND < NOT < comparison) ──

    internal static WhereNode? ParseWhere(ParserContext ctx)
    {
        return ParseOrExpr(ctx);
    }

    private static WhereNode? ParseOrExpr(ParserContext ctx)
    {
        var left = ParseAndExpr(ctx);
        if (left is null || ctx.HasErrors) return null;

        while (ctx.Peek().Type == TokenType.Identifier && ctx.IsKeyword(ctx.Peek(), "or"))
        {
            ctx.Advance();
            var right = ParseAndExpr(ctx);
            if (right is null || ctx.HasErrors) return null;
            left = new LogicalNode { Op = LogicalOp.Or, Left = left, Right = right };
        }

        return left;
    }

    private static WhereNode? ParseAndExpr(ParserContext ctx)
    {
        var left = ParseNotExpr(ctx);
        if (left is null || ctx.HasErrors) return null;

        while (ctx.Peek().Type == TokenType.Identifier && ctx.IsKeyword(ctx.Peek(), "and"))
        {
            ctx.Advance();
            var right = ParseNotExpr(ctx);
            if (right is null || ctx.HasErrors) return null;
            left = new LogicalNode { Op = LogicalOp.And, Left = left, Right = right };
        }

        return left;
    }

    private static WhereNode? ParseNotExpr(ParserContext ctx)
    {
        // Prefix NOT — but only if next-next token is NOT 'between' (that belongs to comparison)
        var token = ctx.Peek();
        if (token.Type == TokenType.Identifier && ctx.IsKeyword(token, "not"))
        {
            // Peek ahead: if the token after 'not' is an identifier that could be a column,
            // and the one after that is NOT 'between', this is prefix NOT.
            // But simpler: prefix NOT is followed by another full expression, not by 'between'.
            // We need to check: is the token *after* the identifier-after-not == 'between'?
            // Actually: prefix NOT applies when the next token is NOT the keyword 'between'.
            // If it's 'between', fall through to ParseComparison where 'not between' is handled.
            var next = ctx.PeekAt(1);
            if (next.Type != TokenType.Identifier || (!ctx.IsKeyword(next, "between") && !ctx.IsKeyword(next, "in")))
            {
                ctx.Advance(); // consume 'not'
                var inner = ParseNotExpr(ctx);
                if (inner is null || ctx.HasErrors) return null;
                return new NotNode { Inner = inner };
            }
        }

        return ParseComparison(ctx);
    }

    private static WhereNode? ParseComparison(ParserContext ctx)
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

        // Check for 'is null' / 'is not null'
        var opToken = ctx.Peek();
        if (opToken.Type == TokenType.Identifier && ctx.IsKeyword(opToken, "is"))
        {
            ctx.Advance();
            var nextToken = ctx.Peek();

            if (nextToken.Type == TokenType.Identifier && ctx.IsKeyword(nextToken, "not"))
            {
                ctx.Advance();
                var nullToken = ctx.Peek();
                if (nullToken.Type != TokenType.Identifier || !ctx.IsKeyword(nullToken, "null"))
                {
                    ctx.AddError(nullToken, ErrorCodes.SYNTAX_ERROR, "expected 'null' after 'is not'");
                    return null;
                }
                ctx.Advance();
                return new NullCheckNode
                {
                    Column = colName,
                    ColumnPosition = colToken.Start,
                    ColumnLength = colToken.Length,
                    IsNot = true,
                };
            }

            if (nextToken.Type == TokenType.Identifier && ctx.IsKeyword(nextToken, "null"))
            {
                ctx.Advance();
                return new NullCheckNode
                {
                    Column = colName,
                    ColumnPosition = colToken.Start,
                    ColumnLength = colToken.Length,
                    IsNot = false,
                };
            }

            ctx.AddError(nextToken, ErrorCodes.SYNTAX_ERROR, "expected 'null' or 'not null' after 'is'");
            return null;
        }

        // Check for 'in [...]'
        if (opToken.Type == TokenType.Identifier && ctx.IsKeyword(opToken, "in"))
        {
            ctx.Advance();
            return ParseInNode(ctx, colToken, colName, isNot: false);
        }

        // Standard comparison operator
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

        if (op is null && opToken.Type == TokenType.Identifier)
        {
            if (ctx.IsKeyword(opToken, "contains"))
                op = CompareOp.Contains;
            else if (ctx.IsKeyword(opToken, "starts"))
                op = CompareOp.StartsWith;
            else if (ctx.IsKeyword(opToken, "ends"))
                op = CompareOp.EndsWith;
            else if (ctx.IsKeyword(opToken, "between"))
                op = CompareOp.Between;
            else if (ctx.IsKeyword(opToken, "not"))
            {
                ctx.Advance();
                var nextToken = ctx.Peek();
                if (nextToken.Type == TokenType.Identifier && ctx.IsKeyword(nextToken, "between"))
                    op = CompareOp.NotBetween;
                else if (nextToken.Type == TokenType.Identifier && ctx.IsKeyword(nextToken, "in"))
                {
                    ctx.Advance(); // consume 'in'
                    return ParseInNode(ctx, colToken, colName, isNot: true);
                }
                else
                {
                    ctx.AddError(opToken, ErrorCodes.SYNTAX_ERROR, "expected 'between' or 'in' after 'not'");
                    return null;
                }
            }
        }

        if (op is null)
        {
            ctx.AddError(opToken, ErrorCodes.SYNTAX_ERROR, "expected comparison operator ('=', '!=', '>', '>=', '<', '<=', 'contains', 'starts', 'ends', 'between', 'not between', 'in', 'not in', 'is null', 'is not null')");
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

        // Between requires 'and <value2>'
        string? value2 = null;
        if (op is CompareOp.Between or CompareOp.NotBetween)
        {
            var andToken = ctx.Peek();
            if (andToken.Type != TokenType.Identifier || !ctx.IsKeyword(andToken, "and"))
            {
                ctx.AddError(andToken, ErrorCodes.SYNTAX_ERROR, "expected 'and' after first value in 'between'");
                return null;
            }
            ctx.Advance();

            var value2Token = ctx.Peek();
            value2 = ParseLiteralValue(ctx, value2Token);
            if (value2 is null)
            {
                ctx.AddError(value2Token, ErrorCodes.SYNTAX_ERROR, ErrorMessages.EXPECTED_VALUE);
                return null;
            }
        }

        return new CompareNode
        {
            Column = colName,
            ColumnPosition = colToken.Start,
            ColumnLength = colToken.Length,
            Operator = op.Value,
            Value = value,
            Value2 = value2,
        };
    }

    private static WhereNode? ParseInNode(ParserContext ctx, Token colToken, string colName, bool isNot)
    {
        var values = ParseValueList(ctx);
        if (ctx.HasErrors) return null;

        return new InNode
        {
            Column = colName,
            ColumnPosition = colToken.Start,
            ColumnLength = colToken.Length,
            Values = values,
            IsNot = isNot,
        };
    }

    private static List<string> ParseValueList(ParserContext ctx)
    {
        var bracketToken = ctx.Peek();
        if (bracketToken.Type != TokenType.LeftBracket)
        {
            ctx.AddError(bracketToken, ErrorCodes.SYNTAX_ERROR, "expected '[' after 'in'");
            return [];
        }
        ctx.Advance();

        var values = new List<string>();

        // Check for empty list
        if (ctx.Peek().Type == TokenType.RightBracket)
        {
            ctx.AddError(ctx.Peek(), ErrorCodes.SYNTAX_ERROR, "value list must contain at least one value");
            return [];
        }

        while (true)
        {
            var valueToken = ctx.Peek();
            var value = ParseLiteralValue(ctx, valueToken);
            if (value is null)
            {
                ctx.AddError(valueToken, ErrorCodes.SYNTAX_ERROR, ErrorMessages.EXPECTED_VALUE);
                return [];
            }
            values.Add(value);

            if (ctx.Peek().Type == TokenType.Comma)
            {
                ctx.Advance();
                continue;
            }

            break;
        }

        var closeBracket = ctx.Peek();
        if (closeBracket.Type != TokenType.RightBracket)
        {
            ctx.AddError(closeBracket, ErrorCodes.SYNTAX_ERROR, "expected ']' to close value list");
            return [];
        }
        ctx.Advance();

        return values;
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
                // Strip quotes and unescape \'
                return ctx.Input.Substring(token.Start + 1, token.Length - 2).Replace("\\'", "'");

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

    private static readonly string[] SelectStopKeywords = ["distinct", "where", "order", "limit", "count", "group", "page", "follow"];

    private static (List<SelectColumn> Columns, List<ComputedColumn>? Computed) ParseSelectList(ParserContext ctx)
    {
        var columns = new List<SelectColumn>();
        List<ComputedColumn>? computed = null;

        while (true)
        {
            var token = ctx.Peek();
            if (token.Type != TokenType.Identifier)
            {
                ctx.AddError(token, ErrorCodes.SYNTAX_ERROR, ErrorMessages.EXPECTED_COLUMN_NAME);
                return (columns, computed);
            }

            // Stop if this identifier is a clause keyword
            if (IsSelectStopKeyword(ctx, token))
                break;

            var colName = ctx.GetLowercaseText(token);

            // Check if next token is an arithmetic operator → computed field
            var next = ctx.PeekAt(1);
            if (IsArithmeticOp(next.Type))
            {
                var comp = ParseComputedColumn(ctx, token, colName);
                if (ctx.HasErrors) return (columns, computed);
                if (comp is not null)
                {
                    computed ??= [];
                    computed.Add(comp);
                }
            }
            else
            {
                ctx.Advance();

                // Check for optional alias: column as alias
                string? alias = null;
                if (ctx.MatchKeyword("as"))
                {
                    var aliasToken = ctx.Peek();
                    if (aliasToken.Type != TokenType.Identifier)
                    {
                        ctx.AddError(aliasToken, ErrorCodes.SYNTAX_ERROR, "expected alias name after 'as'");
                        return (columns, computed);
                    }
                    alias = ctx.GetLowercaseText(aliasToken);
                    ctx.Advance();
                }

                columns.Add(new SelectColumn(colName, token.Start, token.Length, alias));
            }

            if (ctx.Peek().Type == TokenType.Comma)
            {
                ctx.Advance();
                continue;
            }

            break;
        }

        if (columns.Count == 0 && computed is null)
        {
            ctx.AddError(ctx.Peek(), ErrorCodes.SYNTAX_ERROR, ErrorMessages.EXPECTED_COLUMN_NAME);
        }

        return (columns, computed);
    }

    private static bool IsArithmeticOp(TokenType type) =>
        type is TokenType.Plus or TokenType.Minus or TokenType.Star or TokenType.Slash;

    private static ArithmeticOp ToArithmeticOp(TokenType type) => type switch
    {
        TokenType.Plus => ArithmeticOp.Add,
        TokenType.Minus => ArithmeticOp.Subtract,
        TokenType.Star => ArithmeticOp.Multiply,
        TokenType.Slash => ArithmeticOp.Divide,
        _ => ArithmeticOp.Add, // unreachable
    };

    private static ComputedColumn? ParseComputedColumn(ParserContext ctx, Token leftToken, string leftCol)
    {
        ctx.Advance(); // consume left column

        var opToken = ctx.Peek();
        var op = ToArithmeticOp(opToken.Type);
        ctx.Advance(); // consume operator

        // Right operand: column name or numeric literal
        var rightToken = ctx.Peek();
        string? rightColumn = null;
        double? rightLiteral = null;
        var rightPos = rightToken.Start;
        var rightLen = rightToken.Length;

        if (rightToken.Type == TokenType.Identifier)
        {
            rightColumn = ctx.GetLowercaseText(rightToken);
            ctx.Advance();
        }
        else if (rightToken.Type is TokenType.IntegerLiteral or TokenType.FloatLiteral)
        {
            rightLiteral = double.Parse(ctx.GetText(rightToken), System.Globalization.CultureInfo.InvariantCulture);
            ctx.Advance();
        }
        else if (rightToken.Type == TokenType.Minus)
        {
            // Negative literal
            ctx.Advance();
            var numToken = ctx.Peek();
            if (numToken.Type is TokenType.IntegerLiteral or TokenType.FloatLiteral)
            {
                rightLiteral = -double.Parse(ctx.GetText(numToken), System.Globalization.CultureInfo.InvariantCulture);
                rightLen = numToken.Start + numToken.Length - rightPos;
                ctx.Advance();
            }
            else
            {
                ctx.AddError(numToken, ErrorCodes.SYNTAX_ERROR, "expected number after '-'");
                return null;
            }
        }
        else
        {
            ctx.AddError(rightToken, ErrorCodes.SYNTAX_ERROR, "expected column name or number after operator");
            return null;
        }

        // Required: as <alias>
        if (!ctx.MatchKeyword("as"))
        {
            ctx.AddError(ctx.Peek(), ErrorCodes.SYNTAX_ERROR, "computed field requires 'as <alias>'");
            return null;
        }

        var aliasToken = ctx.Peek();
        if (aliasToken.Type != TokenType.Identifier)
        {
            ctx.AddError(aliasToken, ErrorCodes.SYNTAX_ERROR, "expected alias name after 'as'");
            return null;
        }
        var alias = ctx.GetLowercaseText(aliasToken);
        ctx.Advance();

        return new ComputedColumn
        {
            LeftColumn = leftCol,
            LeftPosition = leftToken.Start,
            LeftLength = leftToken.Length,
            Operator = op,
            RightColumn = rightColumn,
            RightPosition = rightPos,
            RightLength = rightLen,
            RightLiteral = rightLiteral,
            Alias = alias,
        };
    }

    private static bool IsSelectStopKeyword(ParserContext ctx, Token token)
    {
        foreach (var kw in SelectStopKeywords)
        {
            if (ctx.IsKeyword(token, kw))
                return true;
        }
        return false;
    }

    // ── Group by ──────────────────────────────────────────────

    private static List<SelectColumn> ParseGroupByList(ParserContext ctx)
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

            // Stop if this identifier is a clause keyword
            if (IsGroupByStopKeyword(ctx, token))
                break;

            columns.Add(new SelectColumn(ctx.GetLowercaseText(token), token.Start, token.Length));
            ctx.Advance();

            if (ctx.Peek().Type == TokenType.Comma)
            {
                ctx.Advance();
                continue;
            }

            break;
        }

        if (columns.Count == 0)
        {
            ctx.AddError(ctx.Peek(), ErrorCodes.SYNTAX_ERROR, ErrorMessages.EXPECTED_COLUMN_NAME);
        }

        return columns;
    }

    private static bool IsGroupByStopKeyword(ParserContext ctx, Token token)
    {
        return ctx.IsKeyword(token, "order") || ctx.IsKeyword(token, "limit")
            || ctx.IsKeyword(token, "page") || ctx.IsKeyword(token, "follow");
    }

    // ── Follow (join) ──────────────────────────────────────────

    /// <summary>
    /// Parses: source_table.source_col -> target_table.target_col as alias [where ...]
    /// The 'follow' keyword has already been consumed.
    /// </summary>
    private static FollowClause? ParseFollowClause(ParserContext ctx, string mainTable)
    {
        // source_table.source_col
        var srcTableToken = ctx.Peek();
        if (srcTableToken.Type != TokenType.Identifier)
        {
            ctx.AddError(srcTableToken, ErrorCodes.SYNTAX_ERROR, "expected source table name after 'follow'");
            return null;
        }
        var srcTable = ctx.GetLowercaseText(srcTableToken);
        ctx.Advance();

        if (ctx.Peek().Type != TokenType.Dot)
        {
            ctx.AddError(ctx.Peek(), ErrorCodes.SYNTAX_ERROR, "expected '.' after source table name");
            return null;
        }
        ctx.Advance();

        var srcColToken = ctx.Peek();
        if (srcColToken.Type != TokenType.Identifier)
        {
            ctx.AddError(srcColToken, ErrorCodes.SYNTAX_ERROR, "expected source column name after '.'");
            return null;
        }
        var srcCol = ctx.GetLowercaseText(srcColToken);
        ctx.Advance();

        // -> / ->? / ?-> / ?->?
        var arrowToken = ctx.Peek();
        var joinType = arrowToken.Type switch
        {
            TokenType.Arrow => JoinType.Inner,
            TokenType.ArrowOptRight => JoinType.Left,
            TokenType.ArrowOptLeft => JoinType.Right,
            TokenType.ArrowOptBoth => JoinType.Outer,
            _ => (JoinType?)null,
        };
        if (joinType is null)
        {
            ctx.AddError(arrowToken, ErrorCodes.SYNTAX_ERROR, "expected '->', '->?', '?->' or '?->?' after source column");
            return null;
        }
        ctx.Advance();

        // target_table.target_col
        var tgtTableToken = ctx.Peek();
        if (tgtTableToken.Type != TokenType.Identifier)
        {
            ctx.AddError(tgtTableToken, ErrorCodes.SYNTAX_ERROR, "expected target table name after '->'");
            return null;
        }
        var tgtTable = ctx.GetLowercaseText(tgtTableToken);
        ctx.Advance();

        if (ctx.Peek().Type != TokenType.Dot)
        {
            ctx.AddError(ctx.Peek(), ErrorCodes.SYNTAX_ERROR, "expected '.' after target table name");
            return null;
        }
        ctx.Advance();

        var tgtColToken = ctx.Peek();
        if (tgtColToken.Type != TokenType.Identifier)
        {
            ctx.AddError(tgtColToken, ErrorCodes.SYNTAX_ERROR, "expected target column name after '.'");
            return null;
        }
        var tgtCol = ctx.GetLowercaseText(tgtColToken);
        ctx.Advance();

        // as alias
        if (!ctx.MatchKeyword("as"))
        {
            ctx.AddError(ctx.Peek(), ErrorCodes.SYNTAX_ERROR, "expected 'as' after target column");
            return null;
        }

        var aliasToken = ctx.Peek();
        if (aliasToken.Type != TokenType.Identifier)
        {
            ctx.AddError(aliasToken, ErrorCodes.SYNTAX_ERROR, "expected alias name after 'as'");
            return null;
        }
        var alias = ctx.GetLowercaseText(aliasToken);
        ctx.Advance();

        // Optional: select clause for this follow (before where)
        List<SelectColumn>? followSelect = null;
        if (ctx.Peek().Type != TokenType.Eof && ctx.IsKeyword(ctx.Peek(), "select"))
        {
            ctx.Advance(); // consume "select"
            followSelect = ParseFollowSelectList(ctx);
            if (ctx.HasErrors) return null;
        }

        // Optional: where clause for this follow
        WhereNode? followWhere = null;
        if (ctx.Peek().Type != TokenType.Eof && ctx.MatchKeyword("where"))
        {
            followWhere = ParseFollowWhere(ctx);
            if (ctx.HasErrors) return null;
        }

        return new FollowClause
        {
            SourceTable = srcTable,
            SourceColumn = srcCol,
            SourceColumnPosition = srcColToken.Start,
            SourceColumnLength = srcColToken.Length,
            TargetTable = tgtTable,
            TargetTablePosition = tgtTableToken.Start,
            TargetTableLength = tgtTableToken.Length,
            TargetColumn = tgtCol,
            TargetColumnPosition = tgtColToken.Start,
            TargetColumnLength = tgtColToken.Length,
            JoinType = joinType.Value,
            Alias = alias,
            Select = followSelect,
            Where = followWhere,
        };
    }

    /// <summary>
    /// Parses the SELECT list for a follow clause. Stops before 'where', 'follow' or EOF.
    /// </summary>
    private static List<SelectColumn> ParseFollowSelectList(ParserContext ctx)
    {
        var columns = new List<SelectColumn>();
        while (ctx.Peek().Type != TokenType.Eof)
        {
            var token = ctx.Peek();
            if (token.Type != TokenType.Identifier)
                break;

            var text = ctx.GetLowercaseText(token);
            // Stop at follow-terminating keywords
            if (text is "where" or "follow")
                break;

            ctx.Advance();

            // Check for optional alias: column as alias
            string? alias = null;
            if (ctx.MatchKeyword("as"))
            {
                var aliasToken = ctx.Peek();
                if (aliasToken.Type != TokenType.Identifier)
                {
                    ctx.AddError(aliasToken, ErrorCodes.SYNTAX_ERROR, "expected alias name after 'as'");
                    return columns;
                }
                alias = ctx.GetLowercaseText(aliasToken);
                ctx.Advance();
            }

            columns.Add(new SelectColumn(text, token.Start, token.Length, alias));

            // Consume optional comma
            if (ctx.Peek().Type == TokenType.Comma)
                ctx.Advance();
        }

        return columns;
    }

    /// <summary>
    /// Parses the WHERE clause for a follow. Stops before 'follow' keyword
    /// so the next follow can be parsed.
    /// </summary>
    private static WhereNode? ParseFollowWhere(ParserContext ctx)
    {
        return ParseFollowOrExpr(ctx);
    }

    private static WhereNode? ParseFollowOrExpr(ParserContext ctx)
    {
        var left = ParseFollowAndExpr(ctx);
        if (left is null || ctx.HasErrors) return null;

        while (ctx.Peek().Type == TokenType.Identifier
               && ctx.IsKeyword(ctx.Peek(), "or")
               && !IsFollowWhereStop(ctx, 1))
        {
            ctx.Advance();
            var right = ParseFollowAndExpr(ctx);
            if (right is null || ctx.HasErrors) return null;
            left = new LogicalNode { Op = LogicalOp.Or, Left = left, Right = right };
        }

        return left;
    }

    private static WhereNode? ParseFollowAndExpr(ParserContext ctx)
    {
        var left = ParseFollowNotExpr(ctx);
        if (left is null || ctx.HasErrors) return null;

        while (ctx.Peek().Type == TokenType.Identifier
               && ctx.IsKeyword(ctx.Peek(), "and")
               && !IsFollowWhereStop(ctx, 1))
        {
            ctx.Advance();
            var right = ParseFollowNotExpr(ctx);
            if (right is null || ctx.HasErrors) return null;
            left = new LogicalNode { Op = LogicalOp.And, Left = left, Right = right };
        }

        return left;
    }

    private static WhereNode? ParseFollowNotExpr(ParserContext ctx)
    {
        var token = ctx.Peek();
        if (token.Type == TokenType.Identifier && ctx.IsKeyword(token, "not"))
        {
            var next = ctx.PeekAt(1);
            if (next.Type != TokenType.Identifier || (!ctx.IsKeyword(next, "between") && !ctx.IsKeyword(next, "in")))
            {
                ctx.Advance();
                var inner = ParseFollowNotExpr(ctx);
                if (inner is null || ctx.HasErrors) return null;
                return new NotNode { Inner = inner };
            }
        }

        return ParseFollowComparison(ctx);
    }

    private static WhereNode? ParseFollowComparison(ParserContext ctx)
    {
        // If current token is 'follow', stop — this where is done
        if (IsFollowWhereStopToken(ctx, ctx.Peek()))
            return null;

        // Delegate to the standard comparison parser
        return ParseComparison(ctx);
    }

    /// <summary>
    /// Checks if the token at offset from current position is a 'follow' keyword,
    /// meaning we should stop parsing the current follow-where.
    /// </summary>
    private static bool IsFollowWhereStop(ParserContext ctx, int offset)
    {
        var token = ctx.PeekAt(offset);
        return IsFollowWhereStopToken(ctx, token);
    }

    private static bool IsFollowWhereStopToken(ParserContext ctx, Token token)
    {
        return token.Type == TokenType.Identifier && ctx.IsKeyword(token, "follow");
    }

    private static List<OrderByColumn> ParseOrderByList(ParserContext ctx)
    {
        var columns = new List<OrderByColumn>();

        while (true)
        {
            var token = ctx.Peek();
            if (token.Type != TokenType.Identifier)
            {
                ctx.AddError(token, ErrorCodes.SYNTAX_ERROR, ErrorMessages.EXPECTED_COLUMN_NAME);
                return columns;
            }

            var name = ctx.GetLowercaseText(token);
            ctx.Advance();

            var descending = false;
            if (ctx.Peek().Type == TokenType.Identifier && ctx.IsKeyword(ctx.Peek(), "desc"))
            {
                descending = true;
                ctx.Advance();
            }
            else if (ctx.Peek().Type == TokenType.Identifier && ctx.IsKeyword(ctx.Peek(), "asc"))
            {
                ctx.Advance(); // asc is default, just consume it
            }

            columns.Add(new OrderByColumn(name, token.Start, token.Length, descending));

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

using SproutDB.Engine.Parsing;
using System.Buffers;
using System.Runtime.CompilerServices;

namespace SproutDB.Engine.Compilation;

internal ref struct Compiler
{
    private readonly ReadOnlySpan<Token> _tokens;
    private int _position;
    private readonly ArrayPool<string> _stringPool;
    private readonly ArrayPool<JoinExpression> _joinPool;
    private readonly ArrayPool<Expression> _expressionPool;
    private readonly ArrayPool<OrderByField> _orderByPool;

    public Compiler(ReadOnlySpan<Token> tokens)
    {
        _tokens = tokens;
        _position = 0;
        _stringPool = ArrayPool<string>.Shared;
        _joinPool = ArrayPool<JoinExpression>.Shared;
        _expressionPool = ArrayPool<Expression>.Shared;
        _orderByPool = ArrayPool<OrderByField>.Shared;
    }

    private readonly Token Current => _position < _tokens.Length ? _tokens[_position] : _tokens[^1]; // EOF token
    private readonly Token Peek(int offset = 1) => (_position + offset) < _tokens.Length ? _tokens[_position + offset] : _tokens[^1];

    public IStatement Parse()
    {
        switch (_tokens.Length)
        {
            case 0:
            case 1 when _tokens[0].Type == TokenType.EOF:
                throw new ParseException("Empty input", 0);
            default:
                return ParseStatement();
        }
    }

    private IStatement ParseStatement()
    {
        return Current.Type switch
        {
            TokenType.Get or TokenType.Count or TokenType.Sum or TokenType.Avg => ParseQueryStatement(),
            TokenType.Upsert => ParseUpsertStatement(),
            TokenType.Delete => ParseDeleteStatement(),
            TokenType.Create => ParseCreateStatement(),
            TokenType.Add => ParseAddStatement(),
            TokenType.Purge => ParsePurgeStatement(),
            TokenType.Drop => ParseDropStatement(),
            TokenType.Update => ParseUpdateStatement(),
            TokenType.Checkout => ParseCheckoutStatement(),
            TokenType.Merge => ParseMergeStatement(),
            TokenType.Protect or TokenType.Unprotect or TokenType.Abandon or TokenType.Reactivate => ParseBranchManagementStatement(),
            TokenType.Revoke or TokenType.Disable or TokenType.Enable or TokenType.List => ParseAuthStatement(),
            TokenType.Backup or TokenType.Restore or TokenType.Explain or TokenType.Respawn => ParseMetaStatement(),
            TokenType.EOF => throw new ParseException("Unexpected end of input", Current.Position),
            _ => throw new ParseException($"Unexpected token '{Current.Value}'", Current.Position)
        };
    }

    private QueryStatement ParseQueryStatement()
    {
        var position = Current.Position;
        var operation = Current.Type switch
        {
            TokenType.Get => QueryOperation.Get,
            TokenType.Count => QueryOperation.Count,
            TokenType.Sum => QueryOperation.Sum,
            TokenType.Avg => QueryOperation.Avg,
            _ => throw new ParseException($"Expected query operation", Current.Position)
        };

        Advance();
        var (table, implicitSelect) = ParseTableForQuery(operation);

        var joins = ParseJoinClauses();
        var where = ParseOptionalWhereClause();
        var groupBy = ParseOptionalGroupByClause();
        var having = ParseOptionalHavingClause();
        var orderBy = ParseOptionalOrderByClause();

        var explicitSelect = ParseOptionalSelectClause();
        var select = !explicitSelect.IsEmpty ? explicitSelect : implicitSelect;

        var pagination = ParseOptionalPaginationClause();

        return new QueryStatement(position, operation, table, joins, where, groupBy, having, orderBy, select, pagination);
    }

    private UpsertStatement ParseUpsertStatement()
    {
        var position = Current.Position;
        Expect(TokenType.Upsert);

        var table = ParseTableExpression();
        var data = ParseJsonExpression();
        string? onField = null;

        if (Current.Type == TokenType.On)
        {
            Advance();
            onField = ExpectIdentifier();
        }

        return new UpsertStatement(position, table, data, onField);
    }

    private DeleteStatement ParseDeleteStatement()
    {
        var position = Current.Position;
        Expect(TokenType.Delete);

        var table = ParseTableExpression();
        var where = ParseOptionalWhereClause();

        return new DeleteStatement(position, table, where);
    }

    private IStatement ParseCreateStatement()
    {
        var position = Current.Position;
        Expect(TokenType.Create);

        return Current.Type switch
        {
            TokenType.Database => ParseCreateDatabaseStatement(position),
            TokenType.Table => ParseCreateTableStatement(position),
            TokenType.Branch => ParseCreateBranchStatement(position),
            TokenType.Alias => ParseCreateAliasStatement(position),
            TokenType.Token => ParseCreateTokenStatement(position),
            _ => throw new ParseException($"Expected 'table', 'branch', 'alias', or 'token' after 'create'", Current.Position)
        };
    }

    private SchemaStatement ParseCreateDatabaseStatement(int position)
    {
        Expect(TokenType.Database);
        string? databaseName = null;
        if (Current.Type == TokenType.Identifier)
        {
            databaseName = ExpectIdentifier();
        }
        return new SchemaStatement(position, SchemaOperation.CreateDatabase, databaseName);
    }

    private SchemaStatement ParseCreateTableStatement(int position)
    {
        Expect(TokenType.Table);
        var tableName = ExpectIdentifier();
        return new SchemaStatement(position, SchemaOperation.CreateTable, null, tableName);
    }

    private BranchStatement ParseCreateBranchStatement(int position)
    {
        Expect(TokenType.Branch);
        var branchName = ExpectIdentifier();

        string? sourceBranch = null;
        if (Current.Type == TokenType.From)
        {
            Advance();
            sourceBranch = ExpectIdentifier();
        }

        return new BranchStatement(position, BranchOperation.Create, branchName, sourceBranch);
    }

    private BranchStatement ParseCreateAliasStatement(int position)
    {
        Expect(TokenType.Alias);
        var aliasName = ExpectIdentifier();

        if (Current.Type == TokenType.For)
        {
            throw new ParseException("'for' keyword not supported in tokenizer - use different syntax", Current.Position);
        }

        Expect(TokenType.Branch);
        var targetBranch = ExpectIdentifier();

        return new BranchStatement(position, BranchOperation.Create, aliasName, targetBranch);
    }

    private AuthStatement ParseCreateTokenStatement(int position)
    {
        Expect(TokenType.Token);
        var tokenName = ExpectString();

        Expression? config = null;
        if (Current.Type == TokenType.With)
        {
            Advance();
            config = ParseJsonExpression();
        }

        return new AuthStatement(position, AuthOperation.CreateToken, tokenName, config);
    }

    private SchemaStatement ParseAddStatement()
    {
        var position = Current.Position;
        Expect(TokenType.Add);
        Expect(TokenType.Column);

        var fieldPath = ParseFieldPath();
        var segments = fieldPath.As<ReadOnlyMemory<string>>().Span;

        if (segments.Length != 2)
            throw new ParseException("Expected table.column format", fieldPath.Position);

        var tableName = segments[0];
        var columnName = segments[1];

        string? dataType = null;
        if (Current.Type == TokenType.Identifier)
        {
            dataType = Current.Value;
            Advance();
        }

        return new SchemaStatement(position, SchemaOperation.AddColumn, null, tableName, columnName, dataType);
    }

    private SchemaStatement ParsePurgeStatement()
    {
        var position = Current.Position;
        Expect(TokenType.Purge);
        Expect(TokenType.Column);

        var fieldPath = ParseFieldPath();
        var segments = fieldPath.As<ReadOnlyMemory<string>>().Span;

        if (segments.Length != 2)
            throw new ParseException("Expected table.column format", fieldPath.Position);

        var tableName = segments[0];
        var columnName = segments[1];

        return new SchemaStatement(position, SchemaOperation.PurgeColumn, null, tableName, columnName);
    }

    private SchemaStatement ParseDropStatement()
    {
        var position = Current.Position;
        Expect(TokenType.Drop);
        Expect(TokenType.Table);

        var tableName = ExpectIdentifier();
        return new SchemaStatement(position, SchemaOperation.DropTable, null, tableName);
    }

    private BranchStatement ParseUpdateStatement()
    {
        var position = Current.Position;
        Expect(TokenType.Update);
        Expect(TokenType.Alias);

        var aliasName = ExpectIdentifier();
        // Simplified: update alias name branch target
        Expect(TokenType.Branch);
        var targetBranch = ExpectIdentifier();

        return new BranchStatement(position, BranchOperation.Create, aliasName, targetBranch); // Reuse Create with different context
    }

    private BranchStatement ParseCheckoutStatement()
    {
        var position = Current.Position;
        Expect(TokenType.Checkout);
        Expect(TokenType.Branch);

        var branchName = ExpectIdentifier();
        string? asOfTime = null;
        string? alias = null;

        if (Current.Type == TokenType.As && Peek().Type == TokenType.Of)
        {
            Advance(); // 'as'
            Advance(); // 'of'
            asOfTime = ExpectString();

            if (Current.Type == TokenType.As)
            {
                Advance();
                alias = ExpectIdentifier();
            }
        }

        return new BranchStatement(position, BranchOperation.Checkout, branchName, asOfTime: asOfTime, alias: alias);
    }

    private BranchStatement ParseMergeStatement()
    {
        var position = Current.Position;
        Expect(TokenType.Merge);

        var sourceBranch = ExpectIdentifier();
        Expect(TokenType.Into);
        var targetBranch = ExpectIdentifier();

        return new BranchStatement(position, BranchOperation.Merge, sourceBranch: sourceBranch, targetBranch: targetBranch);
    }

    private BranchStatement ParseBranchManagementStatement()
    {
        var position = Current.Position;
        var operation = Current.Type switch
        {
            TokenType.Protect => BranchOperation.Protect,
            TokenType.Unprotect => BranchOperation.Unprotect,
            TokenType.Abandon => BranchOperation.Abandon,
            TokenType.Reactivate => BranchOperation.Reactivate,
            _ => throw new ParseException($"Unexpected branch operation", Current.Position)
        };

        Advance();
        Expect(TokenType.Branch);
        var branchName = ExpectIdentifier();

        return new BranchStatement(position, operation, branchName);
    }

    private AuthStatement ParseAuthStatement()
    {
        var position = Current.Position;
        var operation = Current.Type switch
        {
            TokenType.Revoke => AuthOperation.RevokeToken,
            TokenType.Disable => AuthOperation.DisableToken,
            TokenType.Enable => AuthOperation.EnableToken,
            TokenType.List => AuthOperation.ListTokens,
            _ => throw new ParseException($"Unexpected auth operation", Current.Position)
        };

        Advance();

        string? tokenName = null;
        if (operation != AuthOperation.ListTokens)
        {
            Expect(TokenType.Token);
            tokenName = ExpectString();
        }
        else if (Current.Type == TokenType.Token) // list tokens
        {
            Advance(); // consume 'token'
        }

        return new AuthStatement(position, operation, tokenName);
    }

    private MetaStatement ParseMetaStatement()
    {
        var position = Current.Position;
        var operation = Current.Type switch
        {
            TokenType.Backup => MetaOperation.Backup,
            TokenType.Restore => MetaOperation.Restore,
            TokenType.Explain => MetaOperation.Explain,
            TokenType.Respawn => MetaOperation.Respawn,
            _ => throw new ParseException($"Unexpected meta operation", Current.Position)
        };

        Advance();

        string? target = null;
        string? source = null;

        switch (operation)
        {
            case MetaOperation.Backup:
                // backup database to "file.db"
                if (Current.Type == TokenType.Identifier && Current.Value.Equals("database", StringComparison.InvariantCultureIgnoreCase))
                    Advance();
                if (Current.Type == TokenType.Identifier && Current.Value.Equals("to", StringComparison.InvariantCultureIgnoreCase))
                    Advance();
                target = ExpectString();
                break;

            case MetaOperation.Restore:
                // restore database from "file.db"
                if (Current.Type == TokenType.Identifier && Current.Value.Equals("database", StringComparison.InvariantCultureIgnoreCase))
                    Advance();
                if (Current.Type == TokenType.From)
                    Advance();
                source = ExpectString();
                break;

            case MetaOperation.Explain:
                // explain get users where...
                target = "query"; // The rest is another statement, but simplified for now
                break;

            case MetaOperation.Respawn:
                // respawn branch name as new-db
                Expect(TokenType.Branch);
                source = ExpectIdentifier();
                if (Current.Type == TokenType.As)
                {
                    Advance();
                    target = ExpectIdentifier();
                }
                break;
        }

        return new MetaStatement(position, operation, target, source);
    }

    private (TableExpression table, ReadOnlyMemory<Expression> implicitSelect) ParseTableForQuery(QueryOperation operation)
    {
        // Start with empty implicit selection
        ReadOnlyMemory<Expression> implicitSelect = ReadOnlyMemory<Expression>.Empty;

        // Get the table name identifier
        var position = Current.Position;
        var tableIdentifier = ExpectIdentifier();

        // Check if we have a dot notation for aggregate operations
        if (IsAggregateOperation(operation) && Current.Type == TokenType.Dot)
        {
            // This is an aggregate operation with a field reference (avg users.age)
            Advance(); // Consume the dot
            var fieldName = ExpectIdentifier();

            // Create a field path that includes both table and field
            var segments = new[] { tableIdentifier, fieldName };
            var fieldPath = Expression.FieldPath(position, segments);

            // Add this as the implicit selected field
            implicitSelect = new[] { fieldPath }.AsMemory();
        }

        // Handle table alias if present
        string? alias = null;
        if (Current.Type == TokenType.As)
        {
            Advance();
            alias = ExpectIdentifier();
        }

        return (new TableExpression(position, tableIdentifier, alias), implicitSelect);
    }

    private TableExpression ParseTableExpression()
    {
        var position = Current.Position;
        var name = ExpectIdentifier();

        string? alias = null;
        if (Current.Type == TokenType.As)
        {
            Advance();
            alias = ExpectIdentifier();
        }

        return new TableExpression(position, name, alias);
    }

    private ReadOnlyMemory<JoinExpression> ParseJoinClauses()
    {
        if (Current.Type != TokenType.Follow)
            return ReadOnlyMemory<JoinExpression>.Empty;

        var joins = new List<JoinExpression>();

        while (Current.Type == TokenType.Follow)
        {
            joins.Add(ParseJoinExpression());
        }

        return joins.ToArray();
    }

    private JoinExpression ParseJoinExpression()
    {
        var position = Current.Position;
        Expect(TokenType.Follow);

        var leftPath = ParseFieldPath();
        Expect(TokenType.Arrow);
        var rightPath = ParseFieldPath();
        Expect(TokenType.As);
        var alias = ExpectIdentifier();

        var joinType = JoinType.Inner;
        if (Current.Type == TokenType.LeftParen)
        {
            Advance();
            joinType = Current.Type switch
            {
                TokenType.Left => JoinType.Left,
                TokenType.Inner => JoinType.Inner,
                TokenType.Right => JoinType.Right,
                _ => throw new ParseException($"Expected join type", Current.Position)
            };
            Advance();
            Expect(TokenType.RightParen);
        }

        return new JoinExpression(position, leftPath, rightPath, alias, joinType);
    }

    private Expression ParseFieldPath()
    {
        var position = Current.Position;

        // Check if this is an aggregate function reference like count()
        if ((Current.Type == TokenType.Count ||
             Current.Type == TokenType.Sum ||
             Current.Type == TokenType.Avg) &&
            Peek().Type == TokenType.LeftParen)
        {
            // Get function name from the current token
            var functionName = Current.Value.ToLowerInvariant();
            Advance(); // Consume function name
            Advance(); // Consume "("

            // Parse any arguments if present (none for count)
            var arguments = new List<string>();

            // If there's an argument inside the parentheses
            if (Current.Type != TokenType.RightParen)
            {
                // This would be for functions like sum(fieldname)
                if (Current.Type == TokenType.Identifier)
                {
                    arguments.Add(ExpectIdentifier());
                }
            }

            // Expect the closing parenthesis
            Expect(TokenType.RightParen);

            // Create a field path for the aggregate field
            // In the executor, these aggregates are stored as "count()", "sum(fieldname)", etc.
            string fieldName;
            if (arguments.Count > 0)
            {
                fieldName = $"{functionName}({string.Join(",", arguments)})";
            }
            else
            {
                fieldName = $"{functionName}()";
            }

            var functionSegments = new[] { fieldName };
            var functionFieldPathExpr = Expression.FieldPath(position, functionSegments);

            // Check for alias (e.g., "as count")
            if (Current.Type == TokenType.As)
            {
                Advance(); // Consume "as"

                // Get the alias directly from the current token's value
                if (Current.Type == TokenType.Identifier ||
                    Current.Type == TokenType.Count ||
                    Current.Type == TokenType.Sum ||
                    Current.Type == TokenType.Avg)
                {
                    var alias = Current.Value; // Use the token's actual value
                    Advance();
                    return Expression.Alias(position, functionFieldPathExpr, alias);
                }
                else
                {
                    throw new ParseException($"Expected identifier or keyword for alias but found '{Current.Value}'", Current.Position);
                }
            }

            return functionFieldPathExpr;
        }

        // Handle regular field path
        var segments = new List<string>
        {
            ExpectIdentifier()
        };

        while (Current.Type == TokenType.Dot)
        {
            Advance();
            segments.Add(ExpectIdentifier());
        }

        var fieldPathExpr = Expression.FieldPath(position, segments.ToArray());

        // Check for alias (e.g., "field as alias")
        if (Current.Type == TokenType.As)
        {
            Advance(); // Consume "as"

            // Get the alias directly from the current token's value
            if (Current.Type == TokenType.Identifier ||
                Current.Type == TokenType.Count ||
                Current.Type == TokenType.Sum ||
                Current.Type == TokenType.Avg)
            {
                var alias = Current.Value; // Use the token's actual value
                Advance();
                return Expression.Alias(position, fieldPathExpr, alias);
            }
            else
            {
                throw new ParseException($"Expected identifier or keyword for alias but found '{Current.Value}'", Current.Position);
            }
        }

        return fieldPathExpr;
    }

    private Expression? ParseOptionalWhereClause()
    {
        if (Current.Type != TokenType.Where)
            return null;

        Advance();
        return ParseOrExpression();
    }

    private Expression ParseOrExpression()
    {
        var left = ParseAndExpression();

        while (Current.Type == TokenType.Or)
        {
            var position = Current.Position;
            Advance();
            var right = ParseAndExpression();
            left = Expression.Binary(position, LogicalOperator.Or, left, right);
        }

        return left;
    }

    private Expression ParseAndExpression()
    {
        var left = ParseComparisonExpression();

        while (Current.Type == TokenType.And)
        {
            var position = Current.Position;
            Advance();
            var right = ParseComparisonExpression();
            left = Expression.Binary(position, LogicalOperator.And, left, right);
        }

        return left;
    }

    private Expression ParseComparisonExpression()
    {
        var left = ParsePrimaryExpression();

        if (left.Type == ExpressionType.FieldPath &&
            (Current.Type == TokenType.Last || Current.Type == TokenType.This ||
             Current.Type == TokenType.Before || Current.Type == TokenType.After))
        {
            return ParseDateComparisonExpression(left);
        }

        if (IsComparisonOperator(Current.Type))
        {
            var position = Current.Position;
            var op = TokenTypeToComparisonOperator(Current.Type);
            Advance();
            var right = ParsePrimaryExpression();
            return Expression.Comparison(position, op, left, right);
        }

        return left;
    }

    private Expression ParsePrimaryExpression()
    {
        return Current.Type switch
        {
            TokenType.Count or TokenType.Sum or TokenType.Avg when Peek().Type == TokenType.LeftParen => ParseFieldPath(),
            TokenType.Identifier => ParseFieldPath(),
            TokenType.String => ParseStringLiteral(),
            TokenType.Number => ParseNumberLiteral(),
            TokenType.Boolean => ParseBooleanLiteral(),
            TokenType.Null => ParseNullLiteral(),
            TokenType.LeftBracket => ParseArrayLiteral(),
            TokenType.LeftParen => ParseParenthesizedExpression(),
            TokenType.Not => ParseNotExpression(),
            _ => throw new ParseException($"Unexpected token in expression: '{Current.Value}'", Current.Position)
        };
    }
    private Expression ParseDateComparisonExpression(Expression dateField)
    {
        var position = Current.Position;
        var dateOperator = Current.Type;
        Advance();

        switch (dateOperator)
        {
            case TokenType.Last:
                // Handle "last X days/weeks/months/years"
                var amount = int.Parse(ExpectNumber());
                var unit = Current.Type switch
                {
                    TokenType.Days => "days",
                    TokenType.Weeks => "weeks",
                    TokenType.Month => "months", // Handles both singular and plural forms
                    TokenType.Year => "years",
                    _ => throw new ParseException($"Expected time unit (days, weeks, month, year) but found '{Current.Value}'", Current.Position)
                };
                Advance(); // Consume the time unit token

                // Create a date range comparison
                return Expression.Comparison(
                    position,
                    ComparisonOperator.GreaterThanOrEqual,
                    dateField,
                    Expression.Literal(position, LiteralType.Date, $"now-{amount}-{unit}")
                );

            case TokenType.This:
                // Handle "this month/year/week"
                var period = Current.Type switch
                {
                    TokenType.Month => "month",
                    TokenType.Year => "year",
                    TokenType.Weeks => "week", // Assuming the token is the same for singular/plural
                    TokenType.Days => "day",
                    _ => throw new ParseException($"Expected time period (month, year, week, day) but found '{Current.Value}'", Current.Position)
                };
                Advance(); // Consume the period token

                // Create a period-based comparison
                return Expression.Comparison(
                    position,
                    ComparisonOperator.GreaterThanOrEqual,
                    dateField,
                    Expression.Literal(position, LiteralType.Date, $"this-{period}")
                );

            case TokenType.Before:
                // Handle "before 'date'"
                var beforeDate = ParsePrimaryExpression();

                return Expression.Comparison(
                    position,
                    ComparisonOperator.LessThan,
                    dateField,
                    beforeDate
                );

            case TokenType.After:
                // Handle "after 'date'"
                var afterDate = ParsePrimaryExpression();

                return Expression.Comparison(
                    position,
                    ComparisonOperator.GreaterThan,
                    dateField,
                    afterDate
                );

            default:
                throw new ParseException($"Unexpected date operator: {dateOperator}", position);
        }
    }

    private Expression ParseStringLiteral()
    {
        var position = Current.Position;
        var value = Current.Value;
        Advance();
        return Expression.Literal(position, LiteralType.String, value);
    }

    private Expression ParseNumberLiteral()
    {
        var position = Current.Position;
        var value = Current.Value;
        Advance();
        return Expression.Literal(position, LiteralType.Number, value);
    }

    private Expression ParseBooleanLiteral()
    {
        var position = Current.Position;
        var value = Current.Value;
        Advance();
        return Expression.Literal(position, LiteralType.Boolean, value);
    }

    private Expression ParseNullLiteral()
    {
        var position = Current.Position;
        Advance();
        return Expression.Literal(position, LiteralType.Null, "null");
    }

    private Expression ParseArrayLiteral()
    {
        var position = Current.Position;
        Expect(TokenType.LeftBracket);

        var items = new List<Expression>();

        while (Current.Type != TokenType.RightBracket)
        {
            items.Add(ParseJsonValue());

            if (Current.Type == TokenType.Comma)
                Advance();
            else
                break;
        }

        Expect(TokenType.RightBracket);
        return Expression.JsonValue(position, JsonValueType.Array, items.ToArray());
    }

    private Expression ParseParenthesizedExpression()
    {
        Expect(TokenType.LeftParen);
        var expression = ParseOrExpression();
        Expect(TokenType.RightParen);
        return expression;
    }
    private Expression ParseNotExpression()
    {
        var position = Current.Position;
        Advance();
        var operand = ParseComparisonExpression();
        return Expression.Unary(position, LogicalOperator.Not, operand);
    }

    private Expression ParseJsonExpression()
    {
        return Current.Type switch
        {
            TokenType.LeftBrace => ParseJsonObject(),
            TokenType.LeftBracket => ParseJsonArray(),
            _ => ParseJsonValue()
        };
    }

    private Expression ParseJsonObject()
    {
        var position = Current.Position;
        Expect(TokenType.LeftBrace);

        var properties = new Dictionary<string, Expression>();

        while (Current.Type != TokenType.RightBrace)
        {
            var key = Current.Type == TokenType.Identifier ? Current.Value : ExpectString();
            Advance();
            Expect(TokenType.Colon);
            var value = ParseJsonValue();

            properties[key] = value;

            if (Current.Type == TokenType.Comma)
                Advance();
            else
                break;
        }

        Expect(TokenType.RightBrace);
        return Expression.JsonValue(position, JsonValueType.Object, properties);
    }

    private Expression ParseJsonArray()
    {
        var position = Current.Position;
        Expect(TokenType.LeftBracket);

        var items = new List<Expression>();

        while (Current.Type != TokenType.RightBracket)
        {
            items.Add(ParseJsonValue());

            if (Current.Type == TokenType.Comma)
                Advance();
            else
                break;
        }

        Expect(TokenType.RightBracket);
        return Expression.JsonValue(position, JsonValueType.Array, items.ToArray());
    }

    private Expression ParseJsonValue()
    {
        var position = Current.Position;

        return Current.Type switch
        {
            TokenType.String => Expression.JsonValue(position, JsonValueType.String, ExpectString()),
            TokenType.Number => Expression.JsonValue(position, JsonValueType.Number, ExpectNumber()),
            TokenType.Boolean => Expression.JsonValue(position, JsonValueType.Boolean, ExpectBoolean()),
            TokenType.Null => Expression.JsonValue(position, JsonValueType.Null, ExpectNull()),
            TokenType.LeftBrace => ParseJsonObject(),
            TokenType.LeftBracket => ParseJsonArray(),
            _ => throw new ParseException($"Expected JSON value", Current.Position)
        };
    }

    private ReadOnlyMemory<Expression> ParseOptionalGroupByClause()
    {
        if (Current.Type != TokenType.Group)
            return ReadOnlyMemory<Expression>.Empty;

        Advance(); // consume "group"
        Expect(TokenType.By); // consume "by"

        return ParseFieldList();
    }

    private Expression? ParseOptionalHavingClause()
    {
        if (Current.Type != TokenType.Having)
            return null;

        Advance();
        return ParseOrExpression();
    }

    private ReadOnlyMemory<OrderByField> ParseOptionalOrderByClause()
    {
        if (Current.Type != TokenType.Order)
            return ReadOnlyMemory<OrderByField>.Empty;

        Advance(); // consume "order"
        Expect(TokenType.By); // consume "by"

        return ParseOrderByFieldList();
    }

    private ReadOnlyMemory<OrderByField> ParseOrderByFieldList()
    {
        var fields = new List<OrderByField>();

        do
        {
            var field = ParseFieldPath();
            var direction = SortDirection.Asc;

            if (Current.Type is TokenType.Asc or TokenType.Desc)
            {
                direction = Current.Type == TokenType.Asc ? SortDirection.Asc : SortDirection.Desc;
                Advance();
            }

            fields.Add(new OrderByField(field, direction));

            if (Current.Type == TokenType.Comma)
                Advance();
            else
                break;

        } while (true);

        return fields.ToArray();
    }

    private ReadOnlyMemory<Expression> ParseOptionalSelectClause()
    {
        if (Current.Type != TokenType.Select)
            return ReadOnlyMemory<Expression>.Empty;

        Advance();
        return ParseFieldList();
    }

    private ReadOnlyMemory<Expression> ParseFieldList()
    {
        var fields = new List<Expression>();

        do
        {
            fields.Add(ParseFieldPath());

            if (Current.Type == TokenType.Comma)
                Advance();
            else
                break;

        } while (true);

        return fields.ToArray();
    }

    private PaginationExpression? ParseOptionalPaginationClause()
    {
        if (Current.Type != TokenType.Page)
            return null;

        var position = Current.Position;
        Advance(); // "page"

        var page = int.Parse(ExpectNumber());
        Expect(TokenType.Of); // "of"
        Expect(TokenType.Size); // "size"
        var size = int.Parse(ExpectNumber());

        return new PaginationExpression(position, page, size);
    }

    private bool IsAggregateOperation(QueryOperation operation)
    {
        return operation switch
        {
            QueryOperation.Count or QueryOperation.Sum or QueryOperation.Avg => true,
            _ => false
        };
    }

    // Helper Methods
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private void Advance()
    {
        if (_position < _tokens.Length)
            _position++;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private void Expect(TokenType expected)
    {
        if (Current.Type != expected)
        {
            throw new ParseException($"Expected {expected} but found '{Current.Value}'", Current.Position);
        }
        Advance();
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private string ExpectIdentifier()
    {
        if (Current.Type != TokenType.Identifier)
        {
            throw new ParseException($"Expected identifier but found '{Current.Value}'", Current.Position);
        }
        var value = Current.Value;
        Advance();
        return value;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private string ExpectString()
    {
        if (Current.Type != TokenType.String)
        {
            throw new ParseException($"Expected string but found '{Current.Value}'", Current.Position);
        }
        var value = Current.Value;
        Advance();
        return value;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private string ExpectNumber()
    {
        if (Current.Type != TokenType.Number)
        {
            throw new ParseException($"Expected number but found '{Current.Value}'", Current.Position);
        }
        var value = Current.Value;
        Advance();
        return value;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private string ExpectBoolean()
    {
        if (Current.Type != TokenType.Boolean)
        {
            throw new ParseException($"Expected boolean but found '{Current.Value}'", Current.Position);
        }
        var value = Current.Value;
        Advance();
        return value;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private string ExpectNull()
    {
        if (Current.Type != TokenType.Null)
        {
            throw new ParseException($"Expected null but found '{Current.Value}'", Current.Position);
        }
        Advance();
        return "null";
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static bool IsComparisonOperator(TokenType type)
    {
        return type switch
        {
            TokenType.Equals or
            TokenType.NotEquals or
            TokenType.GreaterThan or
            TokenType.GreaterThanOrEqual or
            TokenType.LessThan or
            TokenType.LessThanOrEqual or
            TokenType.In or
            TokenType.Contains or
            TokenType.Any => true,
            _ => false
        };
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static ComparisonOperator TokenTypeToComparisonOperator(TokenType type)
    {
        return type switch
        {
            TokenType.Equals => ComparisonOperator.Equals,
            TokenType.NotEquals => ComparisonOperator.NotEquals,
            TokenType.GreaterThan => ComparisonOperator.GreaterThan,
            TokenType.GreaterThanOrEqual => ComparisonOperator.GreaterThanOrEqual,
            TokenType.LessThan => ComparisonOperator.LessThan,
            TokenType.LessThanOrEqual => ComparisonOperator.LessThanOrEqual,
            TokenType.In => ComparisonOperator.In,
            TokenType.Contains => ComparisonOperator.Contains,
            TokenType.Any => ComparisonOperator.Any,
            _ => throw new ArgumentException($"Not a comparison operator: {type}")
        };
    }
}


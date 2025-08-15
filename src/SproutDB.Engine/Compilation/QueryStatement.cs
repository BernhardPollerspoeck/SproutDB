namespace SproutDB.Engine.Compilation;

public readonly struct QueryStatement : IStatement
{
    public StatementType Type => StatementType.Query;
    public int Position { get; }
    public QueryOperation Operation { get; }
    public TableExpression Table { get; }
    public ReadOnlyMemory<JoinExpression> Joins { get; }
    public Expression? Where { get; }
    public ReadOnlyMemory<Expression> GroupBy { get; }
    public Expression? Having { get; }
    public ReadOnlyMemory<OrderByField> OrderBy { get; }
    public ReadOnlyMemory<Expression> Select { get; }
    public PaginationExpression? Pagination { get; }

    public QueryStatement(int position, QueryOperation operation, TableExpression table,
        ReadOnlyMemory<JoinExpression> joins = default,
        Expression? where = null,
        ReadOnlyMemory<Expression> groupBy = default,
        Expression? having = null,
        ReadOnlyMemory<OrderByField> orderBy = default,
        ReadOnlyMemory<Expression> select = default,
        PaginationExpression? pagination = null)
    {
        Position = position;
        Operation = operation;
        Table = table;
        Joins = joins;
        Where = where;
        GroupBy = groupBy;
        Having = having;
        OrderBy = orderBy;
        Select = select;
        Pagination = pagination;
    }
}


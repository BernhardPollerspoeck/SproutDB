namespace SproutDB.Engine.Compilation;

public readonly struct DeleteStatement(int position, TableExpression table, Expression? where = null) : IStatement
{
    public StatementType Type => StatementType.Delete;
    public int Position { get; } = position;
    public TableExpression Table { get; } = table;
    public Expression? Where { get; } = where;
}


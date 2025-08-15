namespace SproutDB.Engine.Compilation;

public readonly struct UpsertStatement(int position, TableExpression table, Expression data, string? onField = null) : IStatement
{
    public StatementType Type => StatementType.Upsert;
    public int Position { get; } = position;
    public TableExpression Table { get; } = table;
    public Expression Data { get; } = data;
    public string? OnField { get; } = onField;
}


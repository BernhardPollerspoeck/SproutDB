namespace SproutDB.Engine.Compilation;

public readonly struct JoinExpression(
    int position, 
    Expression leftPath, 
    Expression rightPath,
    string alias, 
    JoinType joinType = JoinType.Inner,
    Expression? onCondition = null)
{
    public int Position { get; } = position;
    public Expression LeftPath { get; } = leftPath;
    public Expression RightPath { get; } = rightPath;
    public string Alias { get; } = alias;
    public JoinType JoinType { get; } = joinType;
    public Expression? OnCondition { get; } = onCondition;
}


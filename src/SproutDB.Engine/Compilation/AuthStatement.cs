namespace SproutDB.Engine.Compilation;

public readonly struct AuthStatement(
    int position, 
    AuthOperation operation, 
    string? tokenName = null, 
    Expression? configuration = null) 
    : IStatement
{
    public StatementType Type => StatementType.Auth;
    public int Position { get; } = position;
    public AuthOperation Operation { get; } = operation;
    public string? TokenName { get; } = tokenName;
    public Expression? Configuration { get; } = configuration;
}


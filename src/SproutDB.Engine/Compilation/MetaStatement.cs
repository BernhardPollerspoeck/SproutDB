namespace SproutDB.Engine.Compilation;

public readonly struct MetaStatement(
    int position, 
    MetaOperation operation, 
    string? target = null, 
    string? source = null) 
    : IStatement
{
    public StatementType Type => StatementType.Meta;
    public int Position { get; } = position;
    public MetaOperation Operation { get; } = operation;
    public string? Target { get; } = target;
    public string? Source { get; } = source;
}


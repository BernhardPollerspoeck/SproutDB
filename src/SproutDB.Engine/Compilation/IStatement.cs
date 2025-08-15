namespace SproutDB.Engine.Compilation;

public interface IStatement
{
    StatementType Type { get; }
    int Position { get; }
}


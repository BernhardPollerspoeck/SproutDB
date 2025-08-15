using SproutDB.Engine.Parsing;

namespace SproutDB.Engine.Compilation;

public interface IQueryCompiler
{
    IStatement Compile(IEnumerable<Token> tokens);
}


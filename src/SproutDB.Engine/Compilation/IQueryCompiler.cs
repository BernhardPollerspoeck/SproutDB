using SproutDB.Engine.Parsing;

namespace SproutDB.Engine.Compilation;

public interface IQueryCompiler
{
    Node Compile(IEnumerable<Token> tokens);
}



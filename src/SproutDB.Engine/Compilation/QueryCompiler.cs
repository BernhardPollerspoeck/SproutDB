using SproutDB.Engine.Parsing;
using System.Buffers;

namespace SproutDB.Engine.Compilation;

internal class QueryCompiler : IQueryCompiler
{
    public IStatement Compile(IEnumerable<Token> tokens)
    {
        var compiler = new Compiler(tokens.ToArray());

        var result = compiler.Parse();
        return result;
    }
}


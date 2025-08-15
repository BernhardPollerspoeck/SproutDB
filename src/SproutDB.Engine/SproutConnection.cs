using SproutDB.Engine.Compilation;
using SproutDB.Engine.Execution;
using SproutDB.Engine.Parsing;

namespace SproutDB.Engine;

internal class SproutConnection(IQueryParser queryParser, IQueryCompiler queryCompiler, IQueryExecutor queryExecutor) : ISproutConnection
{
    public ExecutionResult Execute(string query)
    {
        var parsedQuery = queryParser.Parse(query);
        var compiledQuery = queryCompiler.Compile(parsedQuery);
        return queryExecutor.Execute(compiledQuery);
    }
}
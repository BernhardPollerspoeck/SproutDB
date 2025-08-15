using SproutDB.Engine.Compilation;

namespace SproutDB.Engine.Execution;

public interface IQueryExecutor
{
    ExecutionResult Execute(IStatement statement);
    ExecutionResult Execute(IStatement statement, ExecutionContext context);
}

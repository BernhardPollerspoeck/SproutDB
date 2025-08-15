using SproutDB.Engine.Execution;

namespace SproutDB.Engine;

public interface ISproutConnection
{
    ExecutionResult Execute(string query);
}

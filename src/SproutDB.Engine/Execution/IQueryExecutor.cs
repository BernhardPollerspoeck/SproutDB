using SproutDB.Engine.Compilation;

namespace SproutDB.Engine.Execution;

public interface IQueryExecutor
{
    void Execute(Node root);
}



using Microsoft.Extensions.DependencyInjection;
using SproutDB.Engine.Compilation;
using SproutDB.Engine.Core;

namespace SproutDB.Engine.Execution;

internal class QueryExecutor(ISproutDB sproutDB, IServiceProvider serviceProvider) : IQueryExecutor
{
    public void Execute(Node root)
    {
        if (root is CreateNode createNode)
        {
            switch (createNode.Type)
            {
                case ECreateType.Table when createNode.Child is IdentifierNode idN:
                    var tableName = idN.Name;

                    var database = sproutDB.GetCurrentDatabase();
                    if (database == null)
                    {
                        throw new ExecutionException("No current database selected");
                    }

                    // Check if table already exists
                    if (database.Tables.ContainsKey(tableName))
                    {
                        throw new ExecutionException($"Table '{tableName}' already exists");
                    }

                    // Create a new table
                    database.Tables.Add(tableName, new Table());
                    break;

                case ECreateType.Database when createNode.Child is IdentifierNode idN:
                    var dbName = idN.Name;

                    // Check if database already exists
                    if (sproutDB.Databases.ContainsKey(dbName))
                    {
                        throw new ExecutionException($"Database '{dbName}' already exists");
                    }
                    // Create a new database
                    //TODO: propper factory
                    var newDb = serviceProvider.GetRequiredService<IDatabase>();
                    sproutDB.Databases.Add(dbName, newDb);
                    break;

                default:
                    throw new ExecutionException($"Unsupported create type: {createNode.Type}");
            }
        }
        else
        {
            throw new ExecutionException($"Unsupported operation type: {root.GetType().Name}");
        }

    }
}



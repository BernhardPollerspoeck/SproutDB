namespace SproutDB.Core.Execution;

internal static class PurgeDatabaseExecutor
{
    public static SproutResponse Execute(string query, string dbName, string dbPath)
    {
        // Idempotent: database doesn't exist → silent OK
        if (!Directory.Exists(dbPath))
        {
            return new SproutResponse
            {
                Operation = SproutOperation.PurgeDatabase,
                Schema = new SchemaInfo { Database = dbName },
            };
        }

        Directory.Delete(dbPath, recursive: true);

        return new SproutResponse
        {
            Operation = SproutOperation.PurgeDatabase,
            Schema = new SchemaInfo { Database = dbName },
        };
    }
}

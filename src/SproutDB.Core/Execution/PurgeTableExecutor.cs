using SproutDB.Core.Parsing;

namespace SproutDB.Core.Execution;

internal static class PurgeTableExecutor
{
    public static SproutResponse Execute(string query, PurgeTableQuery q, string dbPath)
    {
        var tablePath = Path.Combine(dbPath, q.Table);

        // Idempotent: table doesn't exist → silent OK
        if (!Directory.Exists(tablePath))
        {
            return new SproutResponse
            {
                Operation = SproutOperation.PurgeTable,
                Schema = new SchemaInfo { Table = q.Table },
            };
        }

        Directory.Delete(tablePath, recursive: true);

        return new SproutResponse
        {
            Operation = SproutOperation.PurgeTable,
            Schema = new SchemaInfo { Table = q.Table },
        };
    }
}

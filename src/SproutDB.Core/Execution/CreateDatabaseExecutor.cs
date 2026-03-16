using SproutDB.Core.Parsing;
using SproutDB.Core.Storage;

namespace SproutDB.Core.Execution;

internal static class CreateDatabaseExecutor
{
    public static SproutResponse Execute(string query, string database, string dbPath, int chunkSize = 0)
    {
        if (Directory.Exists(dbPath))
            return ResponseHelper.Error(query, ErrorCodes.DATABASE_EXISTS,
                $"database '{database}' already exists");

        Directory.CreateDirectory(dbPath);

        MetaFile.Write(
            Path.Combine(dbPath, "_meta.bin"),
            DateTime.UtcNow.Ticks,
            chunkSize);

        return new SproutResponse
        {
            Operation = SproutOperation.CreateDatabase,
            Schema = new SchemaInfo { Database = database },
        };
    }
}

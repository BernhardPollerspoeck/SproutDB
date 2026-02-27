using System.IO.Compression;
using SproutDB.Core.Parsing;

namespace SproutDB.Core.Execution;

internal static class RestoreExecutor
{
    /// <summary>
    /// Restores a database from a ZIP backup.
    /// The database directory is recreated from the ZIP contents.
    /// Must be called on the writer thread.
    /// </summary>
    public static SproutResponse Execute(string query, string dbName, string dbPath, string zipPath)
    {
        if (!File.Exists(zipPath))
            return ResponseHelper.Error(query, ErrorCodes.SYNTAX_ERROR,
                $"backup file '{zipPath}' does not exist");

        // If database already exists, wipe it first
        if (Directory.Exists(dbPath))
            Directory.Delete(dbPath, true);

        Directory.CreateDirectory(dbPath);

        ZipFile.ExtractToDirectory(zipPath, dbPath);

        return new SproutResponse
        {
            Operation = SproutOperation.Restore,
            Schema = new SchemaInfo { Database = dbName },
        };
    }
}

using System.IO.Compression;
using SproutDB.Core.Parsing;

namespace SproutDB.Core.Execution;

internal static class BackupExecutor
{
    /// <summary>
    /// Creates a consistent ZIP backup of the database.
    /// Must be called on the writer thread after flushing MMFs.
    /// </summary>
    public static SproutResponse Execute(string query, string dbName, string dbPath)
    {
        if (!Directory.Exists(dbPath))
            return ResponseHelper.Error(query, ErrorCodes.UNKNOWN_DATABASE,
                $"database '{dbName}' does not exist");

        var timestamp = DateTime.UtcNow.ToString("yyyyMMdd_HHmmss");
        var zipName = $"{dbName}_{timestamp}.zip";
        var zipPath = Path.Combine(Path.GetDirectoryName(dbPath) ?? dbPath, zipName);

        // Create ZIP from database directory, excluding WAL.
        // Files are opened with FileShare.ReadWrite because MMFs hold locks on .col files.
        using (var zipStream = new FileStream(zipPath, FileMode.Create, FileAccess.Write, FileShare.None))
        using (var zip = new ZipArchive(zipStream, ZipArchiveMode.Create))
        {
            foreach (var file in Directory.GetFiles(dbPath, "*", SearchOption.AllDirectories))
            {
                // Skip WAL file — it's been flushed and is not needed in backup
                if (Path.GetFileName(file) == "_wal")
                    continue;

                var entryName = Path.GetRelativePath(dbPath, file).Replace('\\', '/');
                var entry = zip.CreateEntry(entryName, CompressionLevel.Optimal);

                using var entryStream = entry.Open();
                using var fileStream = new FileStream(file, FileMode.Open, FileAccess.Read, FileShare.ReadWrite);
                fileStream.CopyTo(entryStream);
            }
        }

        return new SproutResponse
        {
            Operation = SproutOperation.Backup,
            BackupPath = zipPath,
            Schema = new SchemaInfo { Database = dbName },
        };
    }
}

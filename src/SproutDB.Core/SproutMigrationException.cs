namespace SproutDB.Core;

/// <summary>
/// Thrown when a query inside a migration returns an error response.
/// </summary>
public sealed class SproutMigrationException : Exception
{
    public string MigrationName { get; }
    public string Query { get; }
    public string ErrorCode { get; }

    public SproutMigrationException(string migrationName, string query, string errorCode, string message)
        : base($"Migration '{migrationName}' failed on query \"{query}\": [{errorCode}] {message}")
    {
        MigrationName = migrationName;
        Query = query;
        ErrorCode = errorCode;
    }
}

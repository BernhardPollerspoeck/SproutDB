namespace SproutDB.Core;

/// <summary>
/// Thrown when a database name does not match the allowed pattern.
/// Valid names match <c>^[a-zA-Z_][a-zA-Z0-9_]*$</c> (case-insensitive; names are lowercased internally).
/// </summary>
public sealed class InvalidDatabaseNameException : ArgumentException
{
    public const string AllowedPattern = "^[a-zA-Z_][a-zA-Z0-9_]*$";

    public string DatabaseName { get; }

    public InvalidDatabaseNameException(string databaseName, string paramName)
        : base($"invalid database name '{databaseName}'. Allowed pattern: {AllowedPattern}", paramName)
    {
        DatabaseName = databaseName;
    }
}

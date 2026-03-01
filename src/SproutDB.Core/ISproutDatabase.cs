namespace SproutDB.Core;

/// <summary>
/// Represents a single database within SproutDB.
/// </summary>
public interface ISproutDatabase
{
    /// <summary>
    /// The database name.
    /// </summary>
    string Name { get; }

    /// <summary>
    /// Executes a query string against this database.
    /// </summary>
    SproutResponse Query(string query);
}

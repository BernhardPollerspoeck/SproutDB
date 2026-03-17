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
    /// Executes one or more queries (semicolon-separated) against this database.
    /// Returns one SproutResponse per query/transaction.
    /// </summary>
    List<SproutResponse> Query(string query);

    /// <summary>
    /// Subscribes to change notifications for the specified table.
    /// The callback fires after each successful mutation (upsert, delete, schema change).
    /// Dispose the returned handle to unsubscribe.
    /// </summary>
    IDisposable OnChange(string table, Action<SproutResponse> callback);

    /// <summary>
    /// Saves a query to the _saved_queries system table (for Admin UI).
    /// Creates the table automatically if it doesn't exist.
    /// </summary>
    void SaveQuery(string name, string query, bool pinned = false);
}

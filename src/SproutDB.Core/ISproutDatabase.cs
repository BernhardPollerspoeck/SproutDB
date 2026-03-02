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

    /// <summary>
    /// Subscribes to change notifications for the specified table.
    /// The callback fires after each successful mutation (upsert, delete, schema change).
    /// Dispose the returned handle to unsubscribe.
    /// </summary>
    IDisposable OnChange(string table, Action<SproutResponse> callback);
}

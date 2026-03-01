namespace SproutDB.Core;

/// <summary>
/// Controls when a migration is executed.
/// </summary>
public enum MigrationMode
{
    /// <summary>
    /// Runs once and is tracked in the _migrations table. Skipped on subsequent calls.
    /// </summary>
    Once,

    /// <summary>
    /// Runs on every startup. Not tracked — useful for cleanup tasks
    /// (e.g. resetting is_connected = false in an IoT database).
    /// </summary>
    OnStartup,
}

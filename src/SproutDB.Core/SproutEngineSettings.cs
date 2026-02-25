namespace SproutDB.Core;

/// <summary>
/// Configuration for the SproutDB engine.
/// </summary>
public sealed class SproutEngineSettings
{
    /// <summary>
    /// Root directory for all database files.
    /// </summary>
    public required string DataDirectory { get; init; }

    /// <summary>
    /// Interval between automatic flush cycles (MMF flush + WAL truncate).
    /// Default: 5 seconds. Set to <see cref="Timeout.InfiniteTimeSpan"/> to disable.
    /// </summary>
    public TimeSpan FlushInterval { get; init; } = TimeSpan.FromSeconds(5);
}

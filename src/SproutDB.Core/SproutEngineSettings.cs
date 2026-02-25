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

    /// <summary>
    /// Interval for WAL group commit (fsync to durable storage).
    /// Writes are buffered in the OS and fsynced at this interval.
    /// Lower values = less data at risk on crash, higher values = better throughput.
    /// Default: 50ms. Set to <see cref="TimeSpan.Zero"/> for immediate fsync per write.
    /// </summary>
    public TimeSpan WalSyncInterval { get; init; } = TimeSpan.FromMilliseconds(50);
}

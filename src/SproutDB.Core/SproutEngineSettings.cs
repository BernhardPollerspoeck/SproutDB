using SproutDB.Core.AutoIndex;
using SproutDB.Core.Storage;

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

    /// <summary>
    /// Maximum number of records in a single bulk upsert.
    /// Exceeding this limit returns error code BULK_LIMIT.
    /// Default: 100.
    /// </summary>
    public int BulkLimit { get; init; } = 100;

    /// <summary>
    /// Default page size for GET results. When a result exceeds this size,
    /// automatic paging is applied. Also used as default for manual paging
    /// when no explicit size is provided.
    /// Default: 100.
    /// </summary>
    public int DefaultPageSize { get; init; } = 100;

    /// <summary>
    /// Pre-allocation chunk size for index and column files.
    /// Controls how many rows are pre-allocated per growth step.
    /// Default: 10,000.
    /// </summary>
    public int ChunkSize { get; init; } = StorageConstants.CHUNK_SIZE;

    /// <summary>
    /// Configuration for automatic index creation and removal.
    /// </summary>
    public AutoIndexSettings AutoIndex { get; init; } = new();
}

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
    public TimeSpan FlushInterval { get; set; } = TimeSpan.FromSeconds(5);

    /// <summary>
    /// Interval for WAL group commit (fsync to durable storage).
    /// Writes are buffered in the OS and fsynced at this interval.
    /// Lower values = less data at risk on crash, higher values = better throughput.
    /// Default: 50ms. Set to <see cref="TimeSpan.Zero"/> for immediate fsync per write.
    /// </summary>
    public TimeSpan WalSyncInterval { get; set; } = TimeSpan.FromMilliseconds(50);

    /// <summary>
    /// Maximum number of records in a single bulk upsert.
    /// Exceeding this limit returns error code BULK_LIMIT.
    /// Default: 100.
    /// </summary>
    public int BulkLimit { get; set; } = 100;

    /// <summary>
    /// Default page size for GET results. When a result exceeds this size,
    /// automatic paging is applied. Also used as default for manual paging
    /// when no explicit size is provided.
    /// Default: 100.
    /// </summary>
    public int DefaultPageSize { get; set; } = 100;

    /// <summary>
    /// Pre-allocation chunk size for index and column files.
    /// Controls how many rows are pre-allocated per growth step.
    /// Default: 10,000.
    /// </summary>
    public int ChunkSize { get; set; } = StorageConstants.CHUNK_SIZE;

    /// <summary>
    /// Configuration for automatic index creation and removal.
    /// </summary>
    public AutoIndexSettings AutoIndex { get; set; } = new();

    /// <summary>
    /// Master API key for auth bootstrap (key management, grants).
    /// Null means auth is disabled (all queries allowed without key).
    /// </summary>
    public string? MasterKey { get; init; }

    /// <summary>
    /// Interval between TTL cleanup passes.
    /// Each pass cleans one table (round-robin).
    /// Default: 5 minutes. Set to <see cref="Timeout.InfiniteTimeSpan"/> to disable.
    /// </summary>
    public TimeSpan TtlCleanupInterval { get; set; } = TimeSpan.FromMinutes(5);

    /// <summary>
    /// Maximum number of expired rows to delete per TTL cleanup pass.
    /// Default: 1000.
    /// </summary>
    public int TtlCleanupBatchSize { get; set; } = 1000;
}

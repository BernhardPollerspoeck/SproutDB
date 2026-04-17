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

    /// <summary>
    /// Idle-evict threshold: a database that has had no query for this many
    /// seconds is flushed and closed (its WAL + TableHandles are released).
    /// The next query re-opens it. Default: 300 (5 minutes).
    /// </summary>
    public int IdleEvictAfterSeconds { get; set; } = 300;

    /// <summary>
    /// Safety-net cap on the number of simultaneously open databases.
    /// When the cap is reached, a new Acquire evicts the least-recently-used
    /// non-busy database. If all are busy, the cap is softly exceeded
    /// (never blocks). Default: 128.
    /// </summary>
    public int MaxOpenDatabases { get; set; } = 128;

    /// <summary>
    /// Enables Gen2-GC-driven eviction when the process is under memory
    /// pressure (<see cref="MemoryPressureThresholdPercent"/> of the GC's
    /// high-memory-load threshold). When triggered, halves the number of
    /// open non-pinned non-busy databases. Default: true.
    /// </summary>
    public bool EnableMemoryPressureEviction { get; set; } = true;

    /// <summary>
    /// Memory-load percentage above which memory-pressure eviction triggers.
    /// Compared against <c>GCMemoryInfo.MemoryLoadBytes / HighMemoryLoadThresholdBytes</c>.
    /// Default: 80.
    /// </summary>
    public int MemoryPressureThresholdPercent { get; set; } = 80;

    /// <summary>
    /// Interval for the background idle-evict sweep. Default: 30 seconds.
    /// Set to <see cref="Timeout.InfiniteTimeSpan"/> to disable idle eviction.
    /// </summary>
    public TimeSpan IdleEvictInterval { get; set; } = TimeSpan.FromSeconds(30);

    /// <summary>
    /// Safety-net cap on the number of simultaneously open table handles across
    /// all databases. When the cap is reached, opening a new table evicts the
    /// least-recently-used table from a database that is currently not under
    /// an active lease (no query in flight). Tables in busy databases are
    /// never evicted mid-query. Set to 0 to disable. Default: 512.
    /// </summary>
    public int MaxOpenTables { get; set; } = 512;

    /// <summary>
    /// When true, the engine inspects the OS resource limits at startup
    /// (<c>RLIMIT_NOFILE</c>) and lowers <see cref="MaxOpenDatabases"/> and
    /// <see cref="MaxOpenTables"/> if the configured caps would exceed the
    /// FD budget. The caller's values are never raised — auto-tune only
    /// protects against EMFILE on constrained hosts. Default: true.
    /// </summary>
    public bool AutoTuneCaps { get; set; } = true;

    /// <summary>
    /// Used by auto-tune to size the FD budget. Set this to your workload's
    /// rough average: how many tables a typical tenant/database has.
    /// Default: 30.
    /// </summary>
    public int AutoTuneAvgTablesPerDatabase { get; set; } = 30;

    /// <summary>
    /// Used by auto-tune to size the FD budget. Rough average of column +
    /// btree handles opened per table under regular load. Default: 8.
    /// </summary>
    public int AutoTuneAvgHandlesPerTable { get; set; } = 8;
}

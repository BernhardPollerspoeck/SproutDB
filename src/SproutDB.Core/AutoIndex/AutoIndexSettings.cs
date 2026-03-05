namespace SproutDB.Core.AutoIndex;

/// <summary>
/// Configuration for automatic index creation and removal.
/// </summary>
public sealed class AutoIndexSettings
{
    /// <summary>
    /// Minimum fraction of queries that use a column in WHERE/ORDER BY/FOLLOW to consider indexing.
    /// Default: 10%.
    /// </summary>
    public double UsageThreshold { get; init; } = 0.10;

    /// <summary>
    /// Minimum fraction of rows that must be filtered out for an index to be beneficial.
    /// With MMF storage, break-even is around 15-20%.
    /// Default: 25%.
    /// </summary>
    public double SelectivityThreshold { get; init; } = 0.25;

    /// <summary>
    /// Minimum ratio of reads to writes to justify index maintenance cost.
    /// Default: 2.5 (2.5 reads per write).
    /// </summary>
    public double ReadWriteRatio { get; init; } = 2.5;

    /// <summary>
    /// Days after which an unused auto-index is removed.
    /// Default: 30 days.
    /// </summary>
    public int UnusedRetentionDays { get; init; } = 30;

    /// <summary>
    /// Minimum number of queries on a column before auto-index evaluation kicks in.
    /// Prevents premature index creation during the first few queries.
    /// Default: 100.
    /// </summary>
    public int MinimumQueryCount { get; init; } = 100;

    /// <summary>
    /// Whether auto-indexing is enabled.
    /// Default: true.
    /// </summary>
    public bool Enabled { get; init; } = true;
}

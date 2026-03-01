namespace SproutDB.Core.AutoIndex;

/// <summary>
/// Configuration for automatic index creation and removal.
/// </summary>
public sealed class AutoIndexSettings
{
    /// <summary>
    /// Minimum fraction of queries that use a column in WHERE to consider indexing.
    /// Default: 30%.
    /// </summary>
    public double UsageThreshold { get; init; } = 0.30;

    /// <summary>
    /// Minimum selectivity (ratio of distinct values to total rows) required.
    /// Columns with very low selectivity (e.g. booleans) don't benefit from indexing.
    /// Default: 95%.
    /// </summary>
    public double SelectivityThreshold { get; init; } = 0.95;

    /// <summary>
    /// Minimum ratio of reads to writes to justify index maintenance cost.
    /// Default: 3.0 (3 reads per write).
    /// </summary>
    public double ReadWriteRatio { get; init; } = 3.0;

    /// <summary>
    /// Days after which an unused auto-index is removed.
    /// Default: 30 days.
    /// </summary>
    public int UnusedRetentionDays { get; init; } = 30;

    /// <summary>
    /// Whether auto-indexing is enabled.
    /// Default: true.
    /// </summary>
    public bool Enabled { get; init; } = true;
}

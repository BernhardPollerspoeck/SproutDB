namespace SproutDB.Core.AutoIndex;

/// <summary>
/// Evaluates whether an auto-index should be created or removed
/// based on collected metrics and configuration thresholds.
/// </summary>
internal static class AutoIndexEvaluator
{
    /// <summary>
    /// Returns true if metrics indicate an index would be beneficial.
    /// </summary>
    public static bool ShouldCreate(IndexMetrics metrics, AutoIndexSettings settings)
    {
        if (!settings.Enabled)
            return false;

        if (metrics.QueryCount == 0)
            return false;

        // Usage threshold: fraction of queries that hit this column in WHERE
        var usageRatio = (double)metrics.WhereHitCount / metrics.QueryCount;
        if (usageRatio < settings.UsageThreshold)
            return false;

        // Selectivity: if scanned rows >> result rows, an index would help
        if (metrics.ScannedRowsTotal > 0 && metrics.ResultRowsTotal > 0)
        {
            var selectivity = 1.0 - (double)metrics.ResultRowsTotal / metrics.ScannedRowsTotal;
            if (selectivity < settings.SelectivityThreshold)
                return false;
        }

        // Read/write ratio: only index if reads dominate
        if (metrics.WriteCount > 0)
        {
            var rwRatio = (double)metrics.ReadCount / metrics.WriteCount;
            if (rwRatio < settings.ReadWriteRatio)
                return false;
        }

        return true;
    }

    /// <summary>
    /// Returns true if an auto-created index should be removed (unused for too long).
    /// Manual indexes are never auto-removed.
    /// </summary>
    public static bool ShouldRemove(IndexMetrics metrics, AutoIndexSettings settings, DateTime now)
    {
        if (metrics.IsManual)
            return false;

        if (metrics.LastUsedAt is null)
            return false;

        var daysSinceUsed = (now - metrics.LastUsedAt.Value).TotalDays;
        return daysSinceUsed > settings.UnusedRetentionDays;
    }
}

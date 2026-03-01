using System.Collections.Concurrent;

namespace SproutDB.Core.AutoIndex;

/// <summary>
/// Stores per-column index metrics across all tables.
/// Thread-safe via ConcurrentDictionary.
/// </summary>
internal sealed class IndexMetricsStore
{
    private readonly ConcurrentDictionary<(string TablePath, string Column), IndexMetrics> _metrics = new();

    public IndexMetrics GetOrCreate(string tablePath, string column)
    {
        return _metrics.GetOrAdd((tablePath, column), _ => new IndexMetrics());
    }

    public void RecordWhereUsage(string tablePath, string column)
    {
        var m = GetOrCreate(tablePath, column);
        m.IncrementWhereHitCount();
        m.LastUsedAt = DateTime.UtcNow;
    }

    public void RecordQuery(string tablePath)
    {
        foreach (var kvp in _metrics)
        {
            if (kvp.Key.TablePath == tablePath)
                kvp.Value.IncrementQueryCount();
        }
    }

    public void RecordRead(string tablePath, string column)
    {
        var m = GetOrCreate(tablePath, column);
        m.IncrementReadCount();
    }

    public void RecordWrite(string tablePath, string column)
    {
        var m = GetOrCreate(tablePath, column);
        m.IncrementWriteCount();
    }

    public void RecordScanStats(string tablePath, string column, long scanned, long results)
    {
        var m = GetOrCreate(tablePath, column);
        m.AddScannedRows(scanned);
        m.AddResultRows(results);
    }

    public void MarkManual(string tablePath, string column)
    {
        var m = GetOrCreate(tablePath, column);
        m.IsManual = true;
        m.IndexCreatedAt = DateTime.UtcNow;
    }

    public IEnumerable<((string TablePath, string Column) Key, IndexMetrics Metrics)> GetAll()
    {
        foreach (var kvp in _metrics)
            yield return (kvp.Key, kvp.Value);
    }
}

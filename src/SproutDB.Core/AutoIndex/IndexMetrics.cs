namespace SproutDB.Core.AutoIndex;

/// <summary>
/// Per-column metrics for auto-index decision making.
/// Thread-safe via Interlocked operations.
/// </summary>
internal sealed class IndexMetrics
{
    private long _queryCount;
    private long _whereHitCount;
    private long _scannedRowsTotal;
    private long _resultRowsTotal;
    private long _readCount;
    private long _writeCount;

    public DateTime? IndexCreatedAt { get; set; }
    public DateTime? LastUsedAt { get; set; }
    public bool IsManual { get; set; }

    public long QueryCount => Interlocked.Read(ref _queryCount);
    public long WhereHitCount => Interlocked.Read(ref _whereHitCount);
    public long ScannedRowsTotal => Interlocked.Read(ref _scannedRowsTotal);
    public long ResultRowsTotal => Interlocked.Read(ref _resultRowsTotal);
    public long ReadCount => Interlocked.Read(ref _readCount);
    public long WriteCount => Interlocked.Read(ref _writeCount);

    public void IncrementQueryCount() => Interlocked.Increment(ref _queryCount);
    public void IncrementWhereHitCount() => Interlocked.Increment(ref _whereHitCount);
    public void AddScannedRows(long count) => Interlocked.Add(ref _scannedRowsTotal, count);
    public void AddResultRows(long count) => Interlocked.Add(ref _resultRowsTotal, count);
    public void IncrementReadCount() => Interlocked.Increment(ref _readCount);
    public void IncrementWriteCount() => Interlocked.Increment(ref _writeCount);

    public void Load(long queryCount, long whereHitCount, long readCount, long writeCount,
        long scannedTotal, long resultTotal, bool isManual, DateTime? lastUsedAt, DateTime? indexCreatedAt)
    {
        Interlocked.Exchange(ref _queryCount, queryCount);
        Interlocked.Exchange(ref _whereHitCount, whereHitCount);
        Interlocked.Exchange(ref _readCount, readCount);
        Interlocked.Exchange(ref _writeCount, writeCount);
        Interlocked.Exchange(ref _scannedRowsTotal, scannedTotal);
        Interlocked.Exchange(ref _resultRowsTotal, resultTotal);
        IsManual = isManual;
        LastUsedAt = lastUsedAt;
        IndexCreatedAt = indexCreatedAt;
    }
}

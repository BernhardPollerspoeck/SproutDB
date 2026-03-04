namespace SproutDB.Core;

public sealed class SproutMetrics
{
    public TimeSpan Uptime { get; init; }
    public long TotalReads { get; init; }
    public long TotalWrites { get; init; }
    public long StorageSizeBytes { get; init; }
    public long WalSizeBytes { get; init; }
    public int DatabaseCount { get; init; }
    public int TableCount { get; init; }
    public long TotalRows { get; init; }
    public List<DatabaseMetrics> Databases { get; init; } = [];
}

public sealed class DatabaseMetrics
{
    public string Name { get; init; } = string.Empty;
    public int TableCount { get; init; }
    public long TotalRows { get; init; }
    public long StorageSizeBytes { get; init; }
    public List<TableMetrics> Tables { get; init; } = [];
}

public sealed class TableMetrics
{
    public string Name { get; init; } = string.Empty;
    public long RowCount { get; init; }
    public long StorageSizeBytes { get; init; }
    public int ColumnCount { get; init; }
    public int IndexCount { get; init; }
    public DateTime CreatedAt { get; init; }
}

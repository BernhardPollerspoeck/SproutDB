namespace SproutDB.Engine.Execution;

// Enhanced result types for production
public readonly struct ExecutionResult(
    bool success, 
    object? data = null, 
    string? error = null,
    int? rowsAffected = null, 
    int? rowsScanned = null, 
    TimeSpan executionTime = default,
    string? commitId = null)
{
    public bool Success { get; } = success;
    public object? Data { get; } = data;
    public string? Error { get; } = error;
    public int? RowsAffected { get; } = rowsAffected;
    public int? RowsScanned { get; } = rowsScanned;
    public TimeSpan ExecutionTime { get; } = executionTime;
    public string? CommitId { get; } = commitId;

    public static ExecutionResult CreateOk(object? data = null, int? rowsAffected = null,
        int? rowsScanned = null, TimeSpan executionTime = default, string? commitId = null)
        => new(true, data, null, rowsAffected, rowsScanned, executionTime, commitId);

    public static ExecutionResult CreateError(string error, TimeSpan executionTime = default)
        => new(false, null, error, null, null, executionTime);
}

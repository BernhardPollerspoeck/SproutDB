namespace SproutDB.Engine.Execution;

public class ExecutionContext
{
    public string? BranchName { get; set; }
    public string? UserId { get; set; }
    public Dictionary<string, object> Parameters { get; set; } = new();
    public TimeSpan? Timeout { get; set; }
    public bool DryRun { get; set; } = false;
    public int? MaxRows { get; set; }
}

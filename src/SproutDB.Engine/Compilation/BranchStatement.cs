namespace SproutDB.Engine.Compilation;

public readonly struct BranchStatement(
    int position, 
    BranchOperation operation, 
    string? branchName = null,
    string? sourceBranch = null, 
    string? targetBranch = null, 
    string? asOfTime = null, 
    string? alias = null) 
    : IStatement
{
    public StatementType Type => StatementType.Branch;
    public int Position { get; } = position;
    public BranchOperation Operation { get; } = operation;
    public string? BranchName { get; } = branchName;
    public string? SourceBranch { get; } = sourceBranch;
    public string? TargetBranch { get; } = targetBranch;
    public string? AsOfTime { get; } = asOfTime;
    public string? Alias { get; } = alias;
}


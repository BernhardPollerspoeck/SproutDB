namespace SproutDB.Core.Parsing;

internal abstract class WhereNode;

internal sealed class CompareNode : WhereNode
{
    public required string Column { get; init; }
    public required int ColumnPosition { get; init; }
    public required int ColumnLength { get; init; }
    public required CompareOp Operator { get; init; }
    public required string Value { get; init; }
    public string? Value2 { get; init; }
}

internal sealed class NullCheckNode : WhereNode
{
    public required string Column { get; init; }
    public required int ColumnPosition { get; init; }
    public required int ColumnLength { get; init; }
    public bool IsNot { get; init; }
}

internal sealed class LogicalNode : WhereNode
{
    public required LogicalOp Op { get; init; }
    public required WhereNode Left { get; init; }
    public required WhereNode Right { get; init; }
}

internal sealed class NotNode : WhereNode
{
    public required WhereNode Inner { get; init; }
}

internal sealed class InNode : WhereNode
{
    public required string Column { get; init; }
    public required int ColumnPosition { get; init; }
    public required int ColumnLength { get; init; }
    public required List<string> Values { get; init; }
    public bool IsNot { get; init; }
}

internal enum CompareOp : byte
{
    Equal,
    NotEqual,
    GreaterThan,
    GreaterThanOrEqual,
    LessThan,
    LessThanOrEqual,
    Contains,
    StartsWith,
    EndsWith,
    Between,
    NotBetween,
}

internal enum LogicalOp : byte
{
    And,
    Or,
}

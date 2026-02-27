namespace SproutDB.Core.Parsing;

/// <summary>
/// A single comparison condition: column op value.
/// Will be extended later with AND/OR/NOT (#037-#039).
/// </summary>
internal sealed class WhereClause
{
    public required string Column { get; init; }
    public required int ColumnPosition { get; init; }
    public required int ColumnLength { get; init; }
    public required CompareOp Operator { get; init; }
    public required string Value { get; init; }
}

internal enum CompareOp : byte
{
    Equal,
    NotEqual,
    GreaterThan,
    GreaterThanOrEqual,
    LessThan,
    LessThanOrEqual,
}

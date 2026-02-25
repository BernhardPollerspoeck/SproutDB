namespace SproutDB.Core;

/// <summary>
/// Describes a single column in a table schema.
/// </summary>
public sealed class ColumnInfo
{
    public required string Name { get; init; }
    public required string Type { get; init; }
    public int? Size { get; init; }
    public required bool Nullable { get; init; }
    public string? Default { get; init; }
    public required bool Strict { get; init; }
    public bool Auto { get; init; }
}

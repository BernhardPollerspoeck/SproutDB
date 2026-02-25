namespace SproutDB.Core.Parsing;

internal sealed class ColumnDefinition
{
    public required string Name { get; init; }
    public required ColumnType Type { get; init; }
    public required int Size { get; init; }
    public bool Strict { get; init; }
    public string? Default { get; init; }

    public bool IsNullable => Default is null;
    public int EntrySize => 1 + Size; // flag byte + value
}

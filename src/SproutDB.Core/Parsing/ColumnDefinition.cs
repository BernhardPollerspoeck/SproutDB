namespace SproutDB.Core.Parsing;

internal sealed class ColumnDefinition
{
    public required string Name { get; init; }
    public required ColumnType Type { get; init; }
    public required int Size { get; init; }
    public bool Strict { get; init; }
    public string? Default { get; init; }

    /// <summary>
    /// <c>unique</c> modifier: the column gets a unique index right with the table
    /// (only in <c>create table</c>; same as a later <c>create index unique</c>).
    /// </summary>
    public bool Unique { get; init; }

    /// <summary>Element type for array columns (e.g. String for "array string 30").</summary>
    public ColumnType? ElementType { get; init; }

    /// <summary>Element size for array columns (e.g. 30 for "array string 30").</summary>
    public int ElementSize { get; init; }

    public bool IsNullable => Default is null;
    public int EntrySize => 1 + Size; // flag byte + value
}

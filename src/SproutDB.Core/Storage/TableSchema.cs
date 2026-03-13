namespace SproutDB.Core.Storage;

internal sealed class TableSchema
{
    public long CreatedTicks { get; set; }
    public long TtlSeconds { get; set; } // 0 = no table TTL
    public List<ColumnSchemaEntry> Columns { get; set; } = [];
}

internal sealed class ColumnSchemaEntry
{
    public string Name { get; set; } = "";
    public string Type { get; set; } = "";
    public int Size { get; set; }
    public int EntrySize { get; set; }
    public bool Nullable { get; set; }
    public string? Default { get; set; }
    public bool Strict { get; set; }
    public bool IsUnique { get; set; }
}

namespace SproutDB.Core.Storage;

internal sealed class TableSchema
{
    public long CreatedTicks { get; set; }
    public long TtlSeconds { get; set; } // 0 = no table TTL
    public List<ColumnSchemaEntry> Columns { get; set; } = [];
    public int ChunkSize { get; set; } // 0 = use database/engine default
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

    /// <summary>Element type for array columns (e.g. "string").</summary>
    public string? ElementType { get; set; }

    /// <summary>Element size for array columns (e.g. 30 for string arrays).</summary>
    public int ElementSize { get; set; }
}

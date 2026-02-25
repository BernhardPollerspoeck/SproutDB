namespace SproutDB.Core;

/// <summary>
/// Schema information returned by schema and structural operations.
/// Fields are populated based on the operation type; unused fields remain null.
/// </summary>
public sealed class SchemaInfo
{
    public string? Database { get; init; }
    public string? Table { get; init; }
    public List<string>? Tables { get; init; }
    public List<ColumnInfo>? Columns { get; init; }
    public string? Column { get; init; }
    public string? Index { get; init; }
}

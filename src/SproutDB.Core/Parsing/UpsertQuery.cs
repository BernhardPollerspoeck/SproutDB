namespace SproutDB.Core.Parsing;

internal sealed class UpsertQuery : IQuery
{
    public SproutOperation Operation => SproutOperation.Upsert;
    public required string Table { get; init; }
    public required List<List<UpsertField>> Records { get; init; }
    public string? OnColumn { get; init; }
    public required List<long> RowTtlSeconds { get; init; } // per record, 0 = no row TTL
}

internal sealed class UpsertField
{
    public required string Name { get; init; }
    public required UpsertValue Value { get; init; }
    public int Position { get; init; }
    public int Length { get; init; }
}

internal sealed class UpsertValue
{
    public required UpsertValueKind Kind { get; init; }
    public string? Raw { get; init; }
}

internal enum UpsertValueKind : byte
{
    Null,
    String,
    Integer,
    Float,
    Boolean,
    Duration,
}

internal static class UpsertValueKindNames
{
    public static string GetName(UpsertValueKind kind) => kind switch
    {
        UpsertValueKind.Null => "null",
        UpsertValueKind.String => "string",
        UpsertValueKind.Integer => "integer",
        UpsertValueKind.Float => "float",
        UpsertValueKind.Boolean => "boolean",
        UpsertValueKind.Duration => "duration",
        _ => "unknown",
    };
}

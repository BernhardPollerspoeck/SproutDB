namespace SproutDB.Core.Parsing;

internal sealed class UpsertQuery : IQuery
{
    public SproutOperation Operation => SproutOperation.Upsert;
    public required string Table { get; init; }
    public required List<List<UpsertField>> Records { get; init; }
    public string? OnColumn { get; init; }
    public required List<long> RowTtlSeconds { get; init; } // per record, 0 = no row TTL

    /// <summary>
    /// Optional <c>when</c> clause (single record only): the write happens only if
    /// the condition holds for the row found via <c>on</c> / <c>_id</c>.
    /// </summary>
    public UpsertCondition? When { get; init; }

    /// <summary>
    /// Set only by WAL replay: the _id the (single) record was assigned when it
    /// was originally executed. Used if the record resolves to no existing row,
    /// so a replayed insert gets the same _id again.
    /// </summary>
    public ulong? ReplayId { get; set; }

    /// <summary>
    /// Set only by WAL replay. The <c>when</c> condition is not re-evaluated:
    /// the WAL only holds writes whose condition held when they ran.
    /// </summary>
    public bool IsReplay { get; set; }
}

internal enum UpsertConditionKind : byte
{
    /// <summary><c>when &lt;where-expression&gt;</c> — row must exist and match.</summary>
    Expression,
    /// <summary><c>when exists</c> — row must exist (update only).</summary>
    Exists,
    /// <summary><c>when not exists</c> — row must not exist (insert only).</summary>
    NotExists,
}

internal sealed class UpsertCondition
{
    public required UpsertConditionKind Kind { get; init; }
    public WhereNode? Where { get; init; } // only for Expression
    public int Position { get; init; }     // 'when' keyword, for error annotation
    public int Length { get; init; }
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
    Array,
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
        UpsertValueKind.Array => "array",
        _ => "unknown",
    };
}

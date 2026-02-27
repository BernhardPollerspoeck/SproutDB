namespace SproutDB.Core;

/// <summary>
/// Universal response object for all SproutDB operations.
/// All fields are always present — unused fields are null or 0, never omitted.
/// </summary>
public sealed class SproutResponse
{
    public required SproutOperation Operation { get; init; }
    public List<Dictionary<string, object?>>? Data { get; init; }
    public int Affected { get; init; }
    public SchemaInfo? Schema { get; init; }
    public PagingInfo? Paging { get; init; }
    public string? BackupPath { get; init; }
    public List<SproutError>? Errors { get; init; }
    public string? AnnotatedQuery { get; init; }
}

namespace SproutDB.Core;

/// <summary>
/// Paging metadata for results that exceed the configured page size.
/// </summary>
public sealed class PagingInfo
{
    public required int Total { get; init; }
    public required int PageSize { get; init; }
    public required int Page { get; init; }
    public string? Next { get; init; }
}

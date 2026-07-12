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

    /// <summary>
    /// Keyset cursor for the next page (the last _id of this page).
    /// Only set for cursor queries (<c>after '...'</c>); null when no more rows exist.
    /// For cursor responses <see cref="Page"/> is 0 and <see cref="Total"/> is the
    /// number of matching rows from the cursor position onward.
    /// </summary>
    public string? NextCursor { get; init; }
}

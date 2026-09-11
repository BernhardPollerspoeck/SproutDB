namespace SproutDB.Core.Parsing;

internal sealed class DeleteQuery : IQuery
{
    public SproutOperation Operation => SproutOperation.Delete;
    public required string Table { get; init; }
    public required WhereNode Where { get; init; }

    /// <summary>
    /// Optional <c>expect N</c>: the delete only happens if exactly N rows match,
    /// otherwise nothing is deleted and EXPECTATION_FAILED is returned.
    /// </summary>
    public int? Expect { get; init; }

    /// <summary>
    /// Set only by WAL replay. <c>expect</c> is not re-checked: the WAL only
    /// holds deletes whose expectation held when they ran.
    /// </summary>
    public bool IsReplay { get; set; }
}

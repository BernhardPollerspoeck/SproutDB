namespace SproutDB.Core.Parsing;

internal sealed class RestoreQuery : IQuery
{
    public SproutOperation Operation => SproutOperation.Restore;

    /// <summary>
    /// Path to the backup ZIP file.
    /// </summary>
    public required string FilePath { get; init; }
}

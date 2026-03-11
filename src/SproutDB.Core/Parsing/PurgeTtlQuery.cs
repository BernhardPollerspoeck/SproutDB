namespace SproutDB.Core.Parsing;

internal sealed class PurgeTtlQuery : IQuery
{
    public SproutOperation Operation => SproutOperation.PurgeTtl;
    public required string Table { get; init; }
}

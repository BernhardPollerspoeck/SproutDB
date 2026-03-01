namespace SproutDB.Core.Parsing;

internal sealed class UnrestrictQuery : IQuery
{
    public SproutOperation Operation => SproutOperation.Unrestrict;
    public required string Table { get; init; }
    public required string KeyName { get; init; }
    public required string Database { get; init; }
}

namespace SproutDB.Core.Parsing;

internal sealed class RestrictQuery : IQuery
{
    public SproutOperation Operation => SproutOperation.Restrict;
    public required string Table { get; init; }
    public required string Role { get; init; }
    public required string KeyName { get; init; }
    public required string Database { get; init; }
}

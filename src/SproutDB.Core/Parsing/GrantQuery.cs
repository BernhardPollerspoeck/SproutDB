namespace SproutDB.Core.Parsing;

internal sealed class GrantQuery : IQuery
{
    public SproutOperation Operation => SproutOperation.Grant;
    public required string Role { get; init; }
    public required string Database { get; init; }
    public required string KeyName { get; init; }
}

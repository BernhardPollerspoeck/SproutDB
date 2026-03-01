namespace SproutDB.Core.Parsing;

internal sealed class RevokeQuery : IQuery
{
    public SproutOperation Operation => SproutOperation.Revoke;
    public required string Database { get; init; }
    public required string KeyName { get; init; }
}

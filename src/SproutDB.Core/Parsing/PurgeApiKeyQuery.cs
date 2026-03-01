namespace SproutDB.Core.Parsing;

internal sealed class PurgeApiKeyQuery : IQuery
{
    public SproutOperation Operation => SproutOperation.PurgeApiKey;
    public required string Name { get; init; }
}

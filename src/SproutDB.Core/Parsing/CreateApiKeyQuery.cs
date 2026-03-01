namespace SproutDB.Core.Parsing;

internal sealed class CreateApiKeyQuery : IQuery
{
    public SproutOperation Operation => SproutOperation.CreateApiKey;
    public required string Name { get; init; }
}

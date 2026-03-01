namespace SproutDB.Core.Parsing;

internal sealed class RotateApiKeyQuery : IQuery
{
    public SproutOperation Operation => SproutOperation.RotateApiKey;
    public required string Name { get; init; }
}

namespace SproutDB.Core.Parsing;

internal sealed class DescribeQuery : IQuery
{
    public SproutOperation Operation => SproutOperation.Describe;

    /// <summary>
    /// Table name to describe, or null for "describe all tables".
    /// </summary>
    public string? Table { get; init; }
}

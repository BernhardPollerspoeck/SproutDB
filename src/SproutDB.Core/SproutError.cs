namespace SproutDB.Core;

/// <summary>
/// A single error detail with machine-readable code and human-readable message.
/// Optional position info for inline annotation in the query string.
/// </summary>
public sealed class SproutError
{
    public required string Code { get; init; }
    public required string Message { get; init; }
    public int Position { get; init; } = -1;
    public int Length { get; init; }
}

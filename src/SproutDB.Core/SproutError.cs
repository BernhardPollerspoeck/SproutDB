namespace SproutDB.Core;

/// <summary>
/// A single error detail with machine-readable code and human-readable message.
/// </summary>
public sealed class SproutError
{
    public required string Code { get; init; }
    public required string Message { get; init; }
}

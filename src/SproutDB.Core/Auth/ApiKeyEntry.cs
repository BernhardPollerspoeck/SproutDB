namespace SproutDB.Core.Auth;

internal sealed class ApiKeyEntry
{
    public required string Name { get; init; }
    public required string KeyPrefix { get; set; }
    public required string KeyHash { get; set; }
    public DateTime CreatedAt { get; init; }
    public DateTime? LastUsedAt { get; set; }

    /// <summary>
    /// Database → role mapping (e.g. "shop" → "writer").
    /// </summary>
    public Dictionary<string, string> Permissions { get; } = new(StringComparer.OrdinalIgnoreCase);

    /// <summary>
    /// (Database, Table) → role restriction (e.g. ("shop", "orders") → "reader").
    /// </summary>
    public Dictionary<(string Database, string Table), string> Restrictions { get; } = new();
}

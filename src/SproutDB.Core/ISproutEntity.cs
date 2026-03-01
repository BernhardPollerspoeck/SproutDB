namespace SproutDB.Core;

/// <summary>
/// Marker interface for typed table access.
/// Guarantees a ulong Id property mapped to the _id column.
/// </summary>
public interface ISproutEntity
{
    ulong Id { get; set; }
}

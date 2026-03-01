namespace SproutDB.Core.Auth;

/// <summary>
/// Configuration for SproutDB authentication (opt-in).
/// </summary>
public sealed class SproutAuthOptions
{
    /// <summary>
    /// The master API key used for bootstrapping (key management, grants).
    /// Must start with "sdb_ak_".
    /// </summary>
    public required string MasterKey { get; set; }
}

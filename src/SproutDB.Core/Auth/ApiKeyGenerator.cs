using System.Security.Cryptography;
using System.Text;

namespace SproutDB.Core.Auth;

internal static class ApiKeyGenerator
{
    private const string Prefix = "sdb_ak_";
    private const int RandomLength = 32;
    private const string Base62Chars = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";

    /// <summary>
    /// Generates a new API key: sdb_ak_&lt;32 base62 chars&gt;
    /// </summary>
    internal static string Generate()
    {
        Span<byte> randomBytes = stackalloc byte[RandomLength];
        RandomNumberGenerator.Fill(randomBytes);

        var sb = new StringBuilder(Prefix.Length + RandomLength);
        sb.Append(Prefix);

        for (var i = 0; i < RandomLength; i++)
            sb.Append(Base62Chars[randomBytes[i] % Base62Chars.Length]);

        return sb.ToString();
    }

    /// <summary>
    /// SHA-256 hash of the full API key, returned as lowercase hex.
    /// </summary>
    internal static string Hash(string apiKey)
    {
        var bytes = Encoding.UTF8.GetBytes(apiKey);
        var hash = SHA256.HashData(bytes);
        return Convert.ToHexStringLower(hash);
    }

    /// <summary>
    /// Extracts the prefix portion for log identification (e.g. "sdb_ak_a3f2b7c9").
    /// Returns first 15 characters (sdb_ak_ + 8 chars).
    /// </summary>
    internal static string ExtractPrefix(string apiKey)
    {
        const int prefixLength = 15; // "sdb_ak_" (7) + 8 random chars
        return apiKey.Length >= prefixLength
            ? apiKey[..prefixLength]
            : apiKey;
    }
}

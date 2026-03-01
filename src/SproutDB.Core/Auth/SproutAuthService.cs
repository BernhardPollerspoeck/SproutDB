using SproutDB.Core.Execution;
using SproutDB.Core.Parsing;

namespace SproutDB.Core.Auth;

internal sealed class SproutAuthService
{
    private readonly Dictionary<string, ApiKeyEntry> _keysByHash = new(StringComparer.Ordinal);
    private readonly Dictionary<string, ApiKeyEntry> _keysByName = new(StringComparer.OrdinalIgnoreCase);
    private string? _masterKeyHash;

    /// <summary>
    /// Initializes the auth service from _system tables and configures the master key.
    /// Called once during engine startup after EnsureSystemDatabase.
    /// </summary>
    internal void Initialize(SproutEngine engine, string masterKey)
    {
        _masterKeyHash = ApiKeyGenerator.Hash(masterKey);

        // Load _api_keys
        var keysResult = engine.ExecuteInternal("get _api_keys", "_system");
        if (keysResult.Data is not null)
        {
            foreach (var row in keysResult.Data)
            {
                var name = row.GetValueOrDefault("name")?.ToString();
                var prefix = row.GetValueOrDefault("key_prefix")?.ToString();
                var hash = row.GetValueOrDefault("key_hash")?.ToString();

                if (name is null || prefix is null || hash is null)
                    continue;

                var entry = new ApiKeyEntry
                {
                    Name = name,
                    KeyPrefix = prefix,
                    KeyHash = hash,
                    CreatedAt = row.GetValueOrDefault("created_at") is DateTime dt ? dt : DateTime.UtcNow,
                    LastUsedAt = row.GetValueOrDefault("last_used_at") as DateTime?,
                };

                _keysByHash[hash] = entry;
                _keysByName[name] = entry;
            }
        }

        // Load _api_permissions
        var permsResult = engine.ExecuteInternal("get _api_permissions", "_system");
        if (permsResult.Data is not null)
        {
            foreach (var row in permsResult.Data)
            {
                var keyName = row.GetValueOrDefault("key_name")?.ToString();
                var database = row.GetValueOrDefault("database")?.ToString();
                var role = row.GetValueOrDefault("role")?.ToString();

                if (keyName is null || database is null || role is null)
                    continue;

                if (_keysByName.TryGetValue(keyName, out var entry))
                    entry.Permissions[database] = role;
            }
        }

        // Load _api_restrictions
        var restResult = engine.ExecuteInternal("get _api_restrictions", "_system");
        if (restResult.Data is not null)
        {
            foreach (var row in restResult.Data)
            {
                var keyName = row.GetValueOrDefault("key_name")?.ToString();
                var database = row.GetValueOrDefault("database")?.ToString();
                var table = row.GetValueOrDefault("table")?.ToString();
                var role = row.GetValueOrDefault("role")?.ToString();

                if (keyName is null || database is null || table is null || role is null)
                    continue;

                if (_keysByName.TryGetValue(keyName, out var entry))
                    entry.Restrictions[(database, table)] = role;
            }
        }
    }

    /// <summary>
    /// Validates an API key and returns the entry, or null if invalid.
    /// </summary>
    internal ApiKeyEntry? ValidateKey(string apiKey)
    {
        var hash = ApiKeyGenerator.Hash(apiKey);
        return _keysByHash.GetValueOrDefault(hash);
    }

    /// <summary>
    /// Checks if the given API key is the master key.
    /// </summary>
    internal bool IsMasterKey(string apiKey)
    {
        var hash = ApiKeyGenerator.Hash(apiKey);
        return _masterKeyHash is not null && hash == _masterKeyHash;
    }

    /// <summary>
    /// Checks permission for a query. Returns null if allowed, or an error response if denied.
    /// </summary>
    internal SproutResponse? CheckPermission(ApiKeyEntry key, IQuery query, string database)
    {
        var minRole = GetMinimumRole(query);

        // Check database-level permission
        if (!key.Permissions.TryGetValue(database, out var dbRole))
            return PermissionDenied($"no permission on database '{database}'");

        if (RoleLevel(dbRole) < RoleLevel(minRole))
            return PermissionDenied($"role '{dbRole}' on '{database}' is insufficient, requires '{minRole}'");

        // Check table-level restrictions
        var tableName = GetTableName(query);
        if (tableName is not null)
        {
            // Check specific table restriction
            if (key.Restrictions.TryGetValue((database, tableName), out var tableRole))
            {
                if (RoleLevel(tableRole) < RoleLevel(minRole))
                    return PermissionDenied($"restricted to '{tableRole}' on table '{tableName}'");
            }

            // Check wildcard restriction
            if (key.Restrictions.TryGetValue((database, "*"), out var wildcardRole))
            {
                // Specific restriction takes precedence over wildcard
                if (!key.Restrictions.ContainsKey((database, tableName)))
                {
                    if (RoleLevel(wildcardRole) < RoleLevel(minRole))
                        return PermissionDenied($"restricted to '{wildcardRole}' on all tables");
                }
            }
        }

        return null;
    }

    private static SproutResponse PermissionDenied(string detail)
        => ResponseHelper.Error("", ErrorCodes.PERMISSION_DENIED, detail);

    // ── Role hierarchy ───────────────────────────────────────

    private static int RoleLevel(string role) => role.ToLowerInvariant() switch
    {
        "admin" => 3,
        "writer" => 2,
        "reader" => 1,
        "none" => 0,
        _ => -1,
    };

    private static string GetMinimumRole(IQuery query) => query switch
    {
        GetQuery or DescribeQuery => "reader",
        UpsertQuery or DeleteQuery => "writer",
        _ => "admin",
    };

    private static string? GetTableName(IQuery query) => query switch
    {
        GetQuery q => q.Table,
        UpsertQuery q => q.Table,
        DeleteQuery q => q.Table,
        CreateTableQuery q => q.Table,
        PurgeTableQuery q => q.Table,
        AddColumnQuery q => q.Table,
        PurgeColumnQuery q => q.Table,
        RenameColumnQuery q => q.Table,
        AlterColumnQuery q => q.Table,
        CreateIndexQuery q => q.Table,
        PurgeIndexQuery q => q.Table,
        _ => null,
    };

    // ── Cache update methods ─────────────────────────────────

    internal void OnKeyCreated(string name, string prefix, string hash)
    {
        var entry = new ApiKeyEntry
        {
            Name = name,
            KeyPrefix = prefix,
            KeyHash = hash,
            CreatedAt = DateTime.UtcNow,
        };
        _keysByHash[hash] = entry;
        _keysByName[name] = entry;
    }

    internal void OnKeyPurged(string name)
    {
        if (_keysByName.TryGetValue(name, out var entry))
        {
            _keysByHash.Remove(entry.KeyHash);
            _keysByName.Remove(name);
        }
    }

    internal void OnKeyRotated(string name, string newPrefix, string newHash)
    {
        if (_keysByName.TryGetValue(name, out var entry))
        {
            _keysByHash.Remove(entry.KeyHash);
            entry.KeyHash = newHash;
            entry.KeyPrefix = newPrefix;
            _keysByHash[newHash] = entry;
        }
    }

    internal void OnGranted(string keyName, string database, string role)
    {
        if (_keysByName.TryGetValue(keyName, out var entry))
            entry.Permissions[database] = role;
    }

    internal void OnRevoked(string keyName, string database)
    {
        if (_keysByName.TryGetValue(keyName, out var entry))
        {
            entry.Permissions.Remove(database);

            // Also remove all restrictions for this database
            var toRemove = new List<(string, string)>();
            foreach (var key in entry.Restrictions.Keys)
            {
                if (string.Equals(key.Database, database, StringComparison.OrdinalIgnoreCase))
                    toRemove.Add(key);
            }
            foreach (var key in toRemove)
                entry.Restrictions.Remove(key);
        }
    }

    internal void OnRestricted(string keyName, string database, string table, string role)
    {
        if (_keysByName.TryGetValue(keyName, out var entry))
            entry.Restrictions[(database, table)] = role;
    }

    internal void OnUnrestricted(string keyName, string database, string table)
    {
        if (_keysByName.TryGetValue(keyName, out var entry))
            entry.Restrictions.Remove((database, table));
    }

    /// <summary>
    /// Checks if a key name exists in the cache.
    /// </summary>
    internal bool KeyExists(string name) => _keysByName.ContainsKey(name);

    /// <summary>
    /// Gets a key entry by name.
    /// </summary>
    internal ApiKeyEntry? GetKeyByName(string name)
        => _keysByName.GetValueOrDefault(name);

    // ── Auth query helpers ───────────────────────────────────

    /// <summary>
    /// Returns true if the query is an auth management query that should bypass
    /// normal database routing and write protection.
    /// </summary>
    internal static bool IsAuthQuery(IQuery query) => query is
        CreateApiKeyQuery or PurgeApiKeyQuery or RotateApiKeyQuery or
        GrantQuery or RevokeQuery or RestrictQuery or UnrestrictQuery;
}

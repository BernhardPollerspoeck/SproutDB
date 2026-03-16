using System.Reflection;

namespace SproutDB.Core;

internal static class MigrationRunner
{
    private const string MigrationsTable = "_migrations";
    private const string CreateMigrationsTable =
        "create table _migrations (name string 256 strict, migrationorder sint strict, executed datetime strict)";

    public static void Run(Assembly assembly, ISproutDatabase db)
    {
        // Cast to SproutDatabase for internal access (bypasses _ prefix protection)
        var internalDb = (SproutDatabase)db;

        // Ensure _migrations table exists (ignore TABLE_EXISTS)
        var createResult = internalDb.QueryInternal(CreateMigrationsTable);
        if (createResult.Operation == SproutOperation.Error
            && createResult.Errors is not null
            && !createResult.Errors.Any(e => e.Code == "TABLE_EXISTS"))
        {
            throw new InvalidOperationException(
                $"Failed to create _migrations table: {createResult.Errors[0].Message}");
        }

        // Discover all IMigration implementations, sorted by Order
        var migrations = DiscoverMigrations(assembly);

        // Load already-applied Once migrations (reads are not blocked)
        var applied = LoadAppliedMigrations(db);

        // Wrap db in a throwing proxy so query errors inside migrations
        // surface as exceptions instead of being silently swallowed
        var throwingDb = new ThrowingMigrationDatabase(db);

        // Execute in order
        foreach (var migration in migrations)
        {
            var name = migration.GetType().FullName ?? migration.GetType().Name;

            if (migration.Mode == MigrationMode.Once && applied.Contains(name))
                continue;

            // Run the migration — errors throw SproutMigrationException
            throwingDb.CurrentMigrationName = name;
            migration.Up(throwingDb);

            // Track Once migrations (bypasses _ prefix protection)
            if (migration.Mode == MigrationMode.Once)
            {
                var now = DateTime.UtcNow.ToString("yyyy-MM-dd HH:mm:ss");
                var trackQuery = $"upsert {MigrationsTable} {{name: '{EscapeString(name)}', migrationorder: {migration.Order}, executed: '{now}'}}";
                var trackResult = internalDb.QueryInternal(trackQuery);
                if (trackResult.Operation == SproutOperation.Error)
                {
                    throw new SproutMigrationException(
                        name, trackQuery, trackResult.Errors?[0].Code ?? "UNKNOWN",
                        trackResult.Errors?[0].Message ?? "Failed to track migration");
                }
            }
        }
    }

    private static List<IMigration> DiscoverMigrations(Assembly assembly)
    {
        var migrations = new List<IMigration>();

        foreach (var type in assembly.GetTypes())
        {
            if (type.IsAbstract || type.IsInterface)
                continue;

            if (!typeof(IMigration).IsAssignableFrom(type))
                continue;

            var instance = (IMigration?)Activator.CreateInstance(type);
            if (instance is not null)
                migrations.Add(instance);
        }

        migrations.Sort((a, b) => a.Order.CompareTo(b.Order));
        return migrations;
    }

    private static HashSet<string> LoadAppliedMigrations(ISproutDatabase db)
    {
        var result = db.Query($"get {MigrationsTable} select name");
        var applied = new HashSet<string>();

        if (result.Data is null)
            return applied;

        foreach (var row in result.Data)
        {
            if (row.TryGetValue("name", out var nameObj) && nameObj is string name)
                applied.Add(name);
        }

        return applied;
    }

    private static string EscapeString(string value)
    {
        return value.Replace("'", "\\'");
    }

    /// <summary>
    /// Proxy that wraps an ISproutDatabase and throws on any query error,
    /// so migration failures are never silently swallowed.
    /// </summary>
    private sealed class ThrowingMigrationDatabase : ISproutDatabase
    {
        private readonly ISproutDatabase _inner;

        public string Name => _inner.Name;
        public string CurrentMigrationName { get; set; } = "";

        public ThrowingMigrationDatabase(ISproutDatabase inner)
        {
            _inner = inner;
        }

        public SproutResponse Query(string query)
        {
            var result = _inner.Query(query);

            if (result.Operation == SproutOperation.Error && result.Errors is { Count: > 0 })
            {
                var error = result.Errors[0];
                throw new SproutMigrationException(CurrentMigrationName, query, error.Code, error.Message);
            }

            return result;
        }

        public IDisposable OnChange(string table, Action<SproutResponse> callback)
            => _inner.OnChange(table, callback);

        public void SaveQuery(string name, string query, bool pinned = false)
            => _inner.SaveQuery(name, query, pinned);
    }
}

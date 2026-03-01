using System.Reflection;

namespace SproutDB.Core;

/// <summary>
/// Server-level API for managing databases and running migrations.
/// </summary>
public interface ISproutServer
{
    /// <summary>
    /// Returns an existing database or creates it if it doesn't exist.
    /// </summary>
    ISproutDatabase GetOrCreateDatabase(string name);

    /// <summary>
    /// Returns an existing database. Throws if it doesn't exist.
    /// </summary>
    ISproutDatabase SelectDatabase(string name);

    /// <summary>
    /// Returns all databases.
    /// </summary>
    IReadOnlyList<ISproutDatabase> GetDatabases();

    /// <summary>
    /// Scans the assembly for <see cref="IMigration"/> implementations and runs
    /// pending migrations against the given database.
    /// </summary>
    void Migrate(Assembly assembly, ISproutDatabase database);

}

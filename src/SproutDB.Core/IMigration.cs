namespace SproutDB.Core;

/// <summary>
/// Defines a database migration. Implement this interface and place it in an assembly
/// that is passed to <see cref="ISproutServer.Migrate"/>.
/// </summary>
public interface IMigration
{
    /// <summary>
    /// Execution order. Migrations are sorted ascending by this value.
    /// </summary>
    int Order { get; }

    /// <summary>
    /// Controls whether this migration runs once or on every startup.
    /// Default: <see cref="MigrationMode.Once"/>.
    /// </summary>
    MigrationMode Mode => MigrationMode.Once;

    /// <summary>
    /// Executes the migration against the given database.
    /// </summary>
    void Up(ISproutDatabase db);
}

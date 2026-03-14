using SproutDB.Core;

namespace TestMigrations.SilentFailing;

/// <summary>
/// Migration that calls a query returning an error response (UNKNOWN_TABLE).
/// Previously this was swallowed silently — now throws SproutMigrationException.
/// </summary>
public sealed class SilentlyFailingMigration : IMigration
{
    public int Order => 1;

    public void Up(ISproutDatabase db)
    {
        db.Query("add column nonexistent_table.foo string 100");
    }
}

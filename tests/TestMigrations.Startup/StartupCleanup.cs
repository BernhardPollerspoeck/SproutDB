using SproutDB.Core;

namespace TestMigrations.Startup;

public sealed class StartupCleanup : IMigration
{
    public int Order => 2;
    public MigrationMode Mode => MigrationMode.OnStartup;

    public void Up(ISproutDatabase db)
    {
        db.Query("upsert startupcounter {marker: 'started'}");
    }
}

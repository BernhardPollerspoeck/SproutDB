using SproutDB.Core;

namespace TestMigrations.Startup;

public sealed class CreateStartupTable : IMigration
{
    public int Order => 1;

    public void Up(ISproutDatabase db)
    {
        db.Query("create table startupcounter (marker string 50)");
    }
}

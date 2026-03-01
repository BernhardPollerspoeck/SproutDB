using SproutDB.Core;

namespace TestMigrations.Schema;

public sealed class CreateUsers : IMigration
{
    public int Order => 1;

    public void Up(ISproutDatabase db)
    {
        db.Query("create table users (name string 100)");
    }
}

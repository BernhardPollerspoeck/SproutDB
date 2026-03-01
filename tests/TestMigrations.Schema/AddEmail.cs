using SproutDB.Core;

namespace TestMigrations.Schema;

public sealed class AddEmail : IMigration
{
    public int Order => 2;

    public void Up(ISproutDatabase db)
    {
        db.Query("add column users.email string 200");
    }
}

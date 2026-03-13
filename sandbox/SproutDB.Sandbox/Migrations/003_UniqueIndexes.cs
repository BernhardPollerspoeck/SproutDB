using SproutDB.Core;

namespace SproutDB.Sandbox.Migrations;

public sealed class UniqueIndexes : IMigration
{
    public int Order => 3;

    public void Up(ISproutDatabase db)
    {
        db.Query("create index unique customers.email");
        db.Query("create index unique suppliers.contact_email");
        db.Query("create index plants.category");
    }
}

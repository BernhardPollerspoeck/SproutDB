using SproutDB.Core;

namespace TestMigrations.Failing;

public sealed class FailingMigration : IMigration
{
    public int Order => 1;

    public void Up(ISproutDatabase db)
    {
        throw new InvalidOperationException("This migration always fails");
    }
}

namespace SproutDB.Core.Parsing;

internal sealed class CreateDatabaseQuery : IQuery
{
    public SproutOperation Operation => SproutOperation.CreateDatabase;
}

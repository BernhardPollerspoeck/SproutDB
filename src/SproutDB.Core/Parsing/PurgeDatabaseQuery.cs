namespace SproutDB.Core.Parsing;

internal sealed class PurgeDatabaseQuery : IQuery
{
    public SproutOperation Operation => SproutOperation.PurgeDatabase;
}

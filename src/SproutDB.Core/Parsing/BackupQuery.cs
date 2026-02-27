namespace SproutDB.Core.Parsing;

internal sealed class BackupQuery : IQuery
{
    public SproutOperation Operation => SproutOperation.Backup;
}

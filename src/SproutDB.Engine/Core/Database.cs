namespace SproutDB.Engine.Core;

public class Database : IDatabase
{
    public IDictionary<string, Table> Tables { get; } = new Dictionary<string, Table>();
}



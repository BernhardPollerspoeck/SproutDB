using SproutDB.Engine.Core;

namespace SproutDB.Engine;

internal class SproutDB : ISproutDB
{
    public IDictionary<string, IDatabase> Databases { get; } = new Dictionary<string, IDatabase>();

    //TODO: poc
    public IDatabase? GetCurrentDatabase()
    {
        return Databases.Values.FirstOrDefault();
    }
}

using SproutDB.Engine.Core;

namespace SproutDB.Engine;

public interface ISproutDB
{
    IDictionary<string, IDatabase> Databases { get; }
    IDatabase? GetCurrentDatabase();
}

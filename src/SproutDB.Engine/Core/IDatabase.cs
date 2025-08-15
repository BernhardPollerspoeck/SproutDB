
namespace SproutDB.Engine.Core;

public interface IDatabase
{
    IDictionary<string, Table> Tables { get; }
}



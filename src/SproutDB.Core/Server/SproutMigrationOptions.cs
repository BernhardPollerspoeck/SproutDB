using System.Reflection;

namespace SproutDB.Core.Server;

public sealed class SproutMigrationOptions
{
    internal List<(Assembly Assembly, string Database)> Migrations { get; } = [];
}

using SproutDB.Engine.Execution;

namespace SproutDB.Engine.Core;

public class Column(EColumnType type)
{
    public EColumnType Type { get; set; } = type;
}

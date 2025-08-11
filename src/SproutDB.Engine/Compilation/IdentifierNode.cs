namespace SproutDB.Engine.Compilation;

public class IdentifierNode(string name) : Node
{
    public string Name { get; } = name;
}



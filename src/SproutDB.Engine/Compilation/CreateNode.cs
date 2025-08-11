namespace SproutDB.Engine.Compilation;

public class CreateNode : Node
{
    public string Name { get; }
    public ECreateType Type { get; }
    public IdentifierNode Child { get; }
    public CreateNode(string name, ECreateType type, IdentifierNode child)
    {
        Name = name;
        Type = type;
        Child = child;
    }
}



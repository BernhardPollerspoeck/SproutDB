namespace SproutDB.Engine.Compilation;

public readonly struct TableExpression(int position, string name, string? alias = null)
{
    public int Position { get; } = position;
    public string Name { get; } = name;
    public string? Alias { get; } = alias;
}


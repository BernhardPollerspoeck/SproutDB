namespace SproutDB.Engine.Parsing;

public class Token(string value, TokenType type)
{
    public string Value { get; } = value;
    public TokenType Type { get; } = type;

    public override string ToString() => $"[{Type}] {Value}";
}



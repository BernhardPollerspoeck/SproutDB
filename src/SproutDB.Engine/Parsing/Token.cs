namespace SproutDB.Engine.Parsing;

public readonly struct Token(TokenType type, string value, int position)
{
    public TokenType Type { get; } = type;
    public string Value { get; } = value;
    public int Position { get; } = position;
    public int Length { get; } = value.Length;

    // Nur für Error-Messages materialisieren
    public string ToErrorString() => Value.ToString();

    public override string ToString() => $"{Type}: '{Value}' @ {Position}";
}

namespace SproutDB.Core.Parsing;

internal readonly struct Token(TokenType type, int start, int length)
{
    public TokenType Type { get; } = type;
    public int Start { get; } = start;
    public int Length { get; } = length;

    public ReadOnlySpan<char> GetText(ReadOnlySpan<char> input) => input.Slice(Start, Length);
}

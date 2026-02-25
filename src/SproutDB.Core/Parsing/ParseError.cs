namespace SproutDB.Core.Parsing;

internal readonly struct ParseError(int position, int length, string code, string message)
{
    public int Position { get; } = position;
    public int Length { get; } = length;
    public string Code { get; } = code;
    public string Message { get; } = message;
}

namespace SproutDB.Core.Parsing;

internal enum TokenType : byte
{
    Eof,

    // Values
    Identifier,
    StringLiteral,
    IntegerLiteral,
    FloatLiteral,

    // Delimiters
    LeftParen,
    RightParen,
    LeftBrace,
    RightBrace,
    LeftBracket,
    RightBracket,
    Comma,
    Colon,
    Dot,

    // Operators
    Minus,
    Star,
    Equals,
    GreaterThan,
    LessThan,
    GreaterThanOrEqual,
    LessThanOrEqual,
    NotEqual,
    Arrow,
}

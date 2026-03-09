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
    Plus,
    Minus,
    Star,
    Slash,
    Equals,
    GreaterThan,
    LessThan,
    GreaterThanOrEqual,
    LessThanOrEqual,
    NotEqual,
    Arrow,          // ->   (inner join)
    ArrowOptRight,  // ->?  (left join)
    ArrowOptLeft,   // ?->  (right join)
    ArrowOptBoth,   // ?->? (outer join)
}

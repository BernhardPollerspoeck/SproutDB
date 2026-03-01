namespace SproutDB.Core.Parsing;

internal static class Tokenizer
{
    public static List<Token> Tokenize(string input)
    {
        var tokens = new List<Token>();
        var span = input.AsSpan();
        var pos = 0;

        while (pos < span.Length)
        {
            var c = span[pos];

            // Whitespace
            if (char.IsWhiteSpace(c))
            {
                pos++;
                continue;
            }

            // Comment: ## ... ##  (unclosed runs to end of input)
            if (c == '#' && pos + 1 < span.Length && span[pos + 1] == '#')
            {
                pos += 2;
                while (pos + 1 < span.Length)
                {
                    if (span[pos] == '#' && span[pos + 1] == '#')
                    {
                        pos += 2;
                        goto NextChar;
                    }
                    pos++;
                }
                // Unclosed comment — skip remaining
                pos = span.Length;
                continue;
            }

            // String literal: 'text'
            if (c == '\'')
            {
                var start = pos;
                pos++;
                while (pos < span.Length && span[pos] != '\'')
                    pos++;
                if (pos < span.Length)
                    pos++; // closing quote
                // Token spans full literal including quotes
                tokens.Add(new Token(TokenType.StringLiteral, start, pos - start));
                continue;
            }

            // Number: integer or float
            if (char.IsAsciiDigit(c))
            {
                var start = pos;
                var isFloat = false;
                while (pos < span.Length && (char.IsAsciiDigit(span[pos]) || span[pos] == '.'))
                {
                    if (span[pos] == '.')
                    {
                        if (isFloat) break; // second dot → stop
                        isFloat = true;
                    }
                    pos++;
                }
                tokens.Add(new Token(isFloat ? TokenType.FloatLiteral : TokenType.IntegerLiteral, start, pos - start));
                continue;
            }

            // Identifier: starts with letter or _, contains letters/digits/_
            if (char.IsAsciiLetter(c) || c == '_')
            {
                var start = pos;
                while (pos < span.Length && (char.IsAsciiLetterOrDigit(span[pos]) || span[pos] == '_'))
                    pos++;
                tokens.Add(new Token(TokenType.Identifier, start, pos - start));
                continue;
            }

            // Two-character operators
            if (pos + 1 < span.Length)
            {
                var c1 = span[pos + 1];
                if (c == '>' && c1 == '=') { tokens.Add(new Token(TokenType.GreaterThanOrEqual, pos, 2)); pos += 2; continue; }
                if (c == '<' && c1 == '=') { tokens.Add(new Token(TokenType.LessThanOrEqual, pos, 2)); pos += 2; continue; }
                if (c == '!' && c1 == '=') { tokens.Add(new Token(TokenType.NotEqual, pos, 2)); pos += 2; continue; }
                if (c == '-' && c1 == '>') { tokens.Add(new Token(TokenType.Arrow, pos, 2)); pos += 2; continue; }
            }

            // Single-character tokens
            var tokenType = c switch
            {
                '(' => TokenType.LeftParen,
                ')' => TokenType.RightParen,
                '{' => TokenType.LeftBrace,
                '}' => TokenType.RightBrace,
                '[' => TokenType.LeftBracket,
                ']' => TokenType.RightBracket,
                ',' => TokenType.Comma,
                ':' => TokenType.Colon,
                '.' => TokenType.Dot,
                '+' => TokenType.Plus,
                '-' => TokenType.Minus,
                '*' => TokenType.Star,
                '/' => TokenType.Slash,
                '=' => TokenType.Equals,
                '>' => TokenType.GreaterThan,
                '<' => TokenType.LessThan,
                _ => TokenType.Eof, // sentinel for unknown
            };

            if (tokenType != TokenType.Eof)
            {
                tokens.Add(new Token(tokenType, pos, 1));
                pos++;
                continue;
            }

            // Unknown character — skip
            pos++;
            continue;

            NextChar:;
        }

        tokens.Add(new Token(TokenType.Eof, input.Length, 0));
        return tokens;
    }
}

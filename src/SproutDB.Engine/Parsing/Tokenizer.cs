using System.Runtime.CompilerServices;

namespace SproutDB.Engine.Parsing;

public readonly struct Tokenizer(string input)
{
    private static readonly Dictionary<string, TokenType> _keywords = new()
    {
        // Query Operations
        ["get"] = TokenType.Get,
        ["upsert"] = TokenType.Upsert,
        ["delete"] = TokenType.Delete,
        ["count"] = TokenType.Count,
        ["sum"] = TokenType.Sum,
        ["avg"] = TokenType.Avg,

        // Query Clauses
        ["follow"] = TokenType.Follow,
        ["where"] = TokenType.Where,
        ["group"] = TokenType.Group,
        ["by"] = TokenType.By,
        ["having"] = TokenType.Having,
        ["order"] = TokenType.Order,
        ["select"] = TokenType.Select,
        ["as"] = TokenType.As,
        ["on"] = TokenType.On,
        ["for"] = TokenType.For,

        // Pagination
        ["page"] = TokenType.Page,
        ["of"] = TokenType.Of,
        ["size"] = TokenType.Size,

        // Join Types
        ["left"] = TokenType.Left,
        ["inner"] = TokenType.Inner,
        ["right"] = TokenType.Right,

        // Schema Operations
        ["create"] = TokenType.Create,
        ["database"] = TokenType.Database,
        ["drop"] = TokenType.Drop,
        ["add"] = TokenType.Add,
        ["purge"] = TokenType.Purge,
        ["table"] = TokenType.Table,
        ["column"] = TokenType.Column,

        // Branch Operations
        ["branch"] = TokenType.Branch,
        ["commit"] = TokenType.Commit,
        ["checkout"] = TokenType.Checkout,
        ["merge"] = TokenType.Merge,
        ["into"] = TokenType.Into,
        ["from"] = TokenType.From,
        ["alias"] = TokenType.Alias,
        ["update"] = TokenType.Update,
        ["protect"] = TokenType.Protect,
        ["unprotect"] = TokenType.Unprotect,
        ["abandon"] = TokenType.Abandon,
        ["reactivate"] = TokenType.Reactivate,

        // Auth Operations
        ["auth"] = TokenType.Auth,
        ["token"] = TokenType.Token,
        ["with"] = TokenType.With,
        ["disable"] = TokenType.Disable,
        ["enable"] = TokenType.Enable,
        ["revoke"] = TokenType.Revoke,
        ["list"] = TokenType.List,

        // Meta Operations
        ["backup"] = TokenType.Backup,
        ["restore"] = TokenType.Restore,
        ["explain"] = TokenType.Explain,
        ["respawn"] = TokenType.Respawn,
        ["since"] = TokenType.Since,

        // Time/Date Keywords
        ["last"] = TokenType.Last,
        ["this"] = TokenType.This,
        ["days"] = TokenType.Days,
        ["hours"] = TokenType.Hours,
        ["minutes"] = TokenType.Minutes,
        ["weeks"] = TokenType.Weeks,
        ["month"] = TokenType.Month,
        ["year"] = TokenType.Year,
        ["before"] = TokenType.Before,
        ["after"] = TokenType.After,
        ["ago"] = TokenType.Ago,

        // Logical Operators
        ["and"] = TokenType.And,
        ["or"] = TokenType.Or,
        ["not"] = TokenType.Not,

        // Collection Operators
        ["in"] = TokenType.In,
        ["contains"] = TokenType.Contains,
        ["any"] = TokenType.Any,

        // Sorting
        ["asc"] = TokenType.Asc,
        ["desc"] = TokenType.Desc,

        // Special Values
        ["null"] = TokenType.Null,
        ["true"] = TokenType.Boolean,
        ["false"] = TokenType.Boolean,
    };

    public readonly TokenEnumerator GetEnumerator() => new(input);


    public struct TokenEnumerator(string input)
    {
        private readonly string _input = input;
        private int _position = 0;

        public Token Current { get; private set; } = default;

        public bool MoveNext()
        {
            SkipWhitespace();

            if (_position >= _input.Length)
            {
                Current = new Token(TokenType.EOF, string.Empty, _position);
                return false;
            }

            var c = _input[_position];

            // Try single character tokens first
            if (TryReadSingleCharToken(c))
                return true;

            // Try multi-character operators
            if (TryReadMultiCharOperator(c))
                return true;

            // Handle complex tokens
            return c switch
            {
                '"' or '\'' => ReadString(),
                _ when char.IsDigit(c) => ReadNumber(),
                _ when char.IsLetter(c) || c == '_' => ReadIdentifierOrKeyword(),
                '-' when char.IsDigit(PeekChar(1)) => ReadNumber(),
                _ => CreateInvalidToken()
            };
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private bool TryReadSingleCharToken(char c)
        {
            var tokenType = c switch
            {
                '(' => TokenType.LeftParen,
                ')' => TokenType.RightParen,
                '[' => TokenType.LeftBracket,
                ']' => TokenType.RightBracket,
                '{' => TokenType.LeftBrace,
                '}' => TokenType.RightBrace,
                '.' => TokenType.Dot,
                ',' => TokenType.Comma,
                ':' => TokenType.Colon,
                ';' => TokenType.Semicolon,
                '=' => TokenType.Equals,
                _ => TokenType.Invalid
            };

            if (tokenType != TokenType.Invalid)
            {
                CreateTokenAndAdvance(tokenType, 1);
                return true;
            }

            return false;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private bool TryReadMultiCharOperator(char c)
        {
            return c switch
            {
                '!' when PeekChar(1) == '=' => CreateTokenAndAdvance(TokenType.NotEquals, 2),
                '<' when PeekChar(1) == '=' => CreateTokenAndAdvance(TokenType.LessThanOrEqual, 2),
                '<' when PeekChar(1) == '>' => CreateTokenAndAdvance(TokenType.NotEquals, 2),
                '<' => CreateTokenAndAdvance(TokenType.LessThan, 1),
                '>' when PeekChar(1) == '=' => CreateTokenAndAdvance(TokenType.GreaterThanOrEqual, 2),
                '>' => CreateTokenAndAdvance(TokenType.GreaterThan, 1),
                '-' when PeekChar(1) == '>' => CreateTokenAndAdvance(TokenType.Arrow, 2),
                _ => false
            };
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private bool CreateTokenAndAdvance(TokenType type, int length)
        {
            var start = _position;
            _position += length;
            Current = new Token(type, _input.Substring(start, length), start);
            return true;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private bool CreateInvalidToken()
        {
            var start = _position;
            _position++;
            Current = new Token(TokenType.Invalid, _input.Substring(start, 1), start);
            return true;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void SkipWhitespace()
        {
            while (_position < _input.Length && char.IsWhiteSpace(_input[_position]))
                _position++;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private readonly char PeekChar(int offset = 0)
        {
            var pos = _position + offset;
            return pos < _input.Length ? _input[pos] : '\0';
        }

        private bool ReadString()
        {
            var quote = _input[_position];
            var start = _position;
            _position++; // Skip opening quote

            while (_position < _input.Length && _input[_position] != quote)
            {
                if (_input[_position] == '\\' && _position + 1 < _input.Length)
                    _position += 2; // Skip escaped character
                else
                    _position++;
            }

            if (_position < _input.Length)
                _position++; // Skip closing quote

            Current = new Token(TokenType.String, _input[(start + 1)..(_position - 1)], start);
            return true;
        }

        private bool ReadNumber()
        {
            var start = _position;

            // Handle negative sign
            if (_input[_position] == '-')
                _position++;

            // Read integer part
            while (_position < _input.Length && char.IsDigit(_input[_position]))
                _position++;

            // Read decimal part
            if (_position < _input.Length && _input[_position] == '.' &&
                _position + 1 < _input.Length && char.IsDigit(_input[_position + 1]))
            {
                _position++; // Skip dot
                while (_position < _input.Length && char.IsDigit(_input[_position]))
                    _position++;
            }

            Current = new Token(TokenType.Number, _input[start.._position], start);
            return true;
        }

        private bool ReadIdentifierOrKeyword()
        {
            var start = _position;

            while (_position < _input.Length &&
                   (char.IsLetterOrDigit(_input[_position]) || _input[_position] == '_'))
                _position++;

            var span = _input[start.._position];

            // Check if it's a keyword (case-insensitive)
            var str = span.ToString().ToLowerInvariant();
            Current = Tokenizer._keywords.TryGetValue(str, out var tokenType)
                ? new Token(tokenType, span, start)
                : new Token(TokenType.Identifier, span, start);

            return true;
        }
    }
}
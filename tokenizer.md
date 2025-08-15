using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;

namespace SpoutDB.Parsing
{


    public ref struct Tokenizer
    {
        private readonly ReadOnlySpan<char> _input;
        private int _position;
        
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
            ["group"] = TokenType.GroupBy,
            ["by"] = TokenType.GroupBy, // handled in context
            ["having"] = TokenType.Having,
            ["order"] = TokenType.OrderBy,
            ["select"] = TokenType.Select,
            ["as"] = TokenType.As,
            ["on"] = TokenType.On,
            
            // Join Types
            ["left"] = TokenType.Left,
            ["inner"] = TokenType.Inner,
            ["right"] = TokenType.Right,
            
            // Schema Operations
            ["create"] = TokenType.Create,
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
            
            // Auth Operations
            ["auth"] = TokenType.Auth,
            ["token"] = TokenType.Token,
            ["with"] = TokenType.With,
            
            // Time/Date Keywords
            ["last"] = TokenType.Last,
            ["this"] = TokenType.This,
            ["days"] = TokenType.Days,
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
        
        public Tokenizer(ReadOnlySpan<char> input)
        {
            _input = input;
            _position = 0;
        }
        
        public TokenEnumerator GetEnumerator() => new TokenEnumerator(_input);
    }

    public ref struct TokenEnumerator
    {
        private readonly ReadOnlySpan<char> _input;
        private int _position;
        private Token _current;
        
        public TokenEnumerator(ReadOnlySpan<char> input)
        {
            _input = input;
            _position = 0;
            _current = default;
        }
        
        public Token Current => _current;
        
        public bool MoveNext()
        {
            SkipWhitespace();
            
            if (_position >= _input.Length)
            {
                _current = new Token(TokenType.EOF, ReadOnlySpan<char>.Empty, _position);
                return false;
            }
            
            char c = _input[_position];
            
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
            int start = _position;
            _position += length;
            _current = new Token(type, _input.Slice(start, length), start);
            return true;
        }
        
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private bool CreateInvalidToken()
        {
            int start = _position;
            _position++;
            _current = new Token(TokenType.Invalid, _input.Slice(start, 1), start);
            return true;
        }
        
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void SkipWhitespace()
        {
            while (_position < _input.Length && char.IsWhiteSpace(_input[_position]))
                _position++;
        }
        
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private char PeekChar(int offset = 0)
        {
            int pos = _position + offset;
            return pos < _input.Length ? _input[pos] : '\0';
        }
        
        private bool ReadString()
        {
            char quote = _input[_position];
            int start = _position;
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
            
            _current = new Token(TokenType.String, _input.Slice(start, _position - start), start);
            return true;
        }
        
        private bool ReadNumber()
        {
            int start = _position;
            
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
            
            _current = new Token(TokenType.Number, _input.Slice(start, _position - start), start);
            return true;
        }
        
        private bool ReadIdentifierOrKeyword()
        {
            int start = _position;
            
            while (_position < _input.Length && 
                   (char.IsLetterOrDigit(_input[_position]) || _input[_position] == '_'))
                _position++;
            
            var span = _input.Slice(start, _position - start);
            
            // Check if it's a keyword (case-insensitive)
            var str = span.ToString().ToLowerInvariant();
            if (Tokenizer._keywords.TryGetValue(str, out var tokenType))
            {
                _current = new Token(tokenType, span, start);
            }
            else
            {
                _current = new Token(TokenType.Identifier, span, start);
            }
            
            return true;
        }
    }
}
using System.Text;

namespace SproutDB.Core.Parsing;

internal sealed class ParserContext
{
    private readonly string _input;
    private readonly List<Token> _tokens;
    private int _pos;
    private List<ParseError>? _errors;

    public ParserContext(string input, List<Token> tokens)
    {
        _input = input;
        _tokens = tokens;
    }

    public string Input => _input;

    // ── Token helpers ────────────────────────────────────────

    public Token Peek() => _tokens[_pos];

    public Token PeekAt(int offset)
    {
        var index = _pos + offset;
        return index < _tokens.Count ? _tokens[index] : _tokens[^1]; // last is always Eof
    }

    public Token Advance()
    {
        var token = _tokens[_pos];
        if (token.Type != TokenType.Eof)
            _pos++;
        return token;
    }

    public bool MatchKeyword(string keyword)
    {
        var token = Peek();
        if (token.Type == TokenType.Identifier && IsKeyword(token, keyword))
        {
            Advance();
            return true;
        }
        return false;
    }

    public bool IsKeyword(Token token, string keyword)
    {
        return token.Type == TokenType.Identifier
            && token.Length == keyword.Length
            && _input.AsSpan(token.Start, token.Length).Equals(keyword, StringComparison.OrdinalIgnoreCase);
    }

    public bool TryMatchColumnType(Token token)
    {
        return token.Type == TokenType.Identifier
            && ColumnTypes.TryParse(_input.AsSpan(token.Start, token.Length), out _);
    }

    public string GetText(Token token) => _input.Substring(token.Start, token.Length);

    /// <summary>
    /// Gets the text of a string literal token, stripping the surrounding quotes.
    /// </summary>
    public string GetStringLiteralText(Token token) => _input.Substring(token.Start + 1, token.Length - 2).Replace("\\'", "'");

    public string GetLowercaseText(Token token)
    {
        return string.Create(token.Length, (Input: _input, token.Start), static (span, state) =>
        {
            state.Input.AsSpan(state.Start, span.Length).CopyTo(span);
            for (var i = 0; i < span.Length; i++)
                span[i] = char.ToLowerInvariant(span[i]);
        });
    }

    public void ExpectEof()
    {
        var token = Peek();
        if (token.Type != TokenType.Eof)
            AddError(token, ErrorCodes.SYNTAX_ERROR, ErrorMessages.UNEXPECTED_TOKEN_EXPECTED_EOF);
    }

    // ── Error handling ───────────────────────────────────────

    public bool HasErrors => _errors is not null;

    public void AddError(Token token, string code, string message)
    {
        _errors ??= [];
        _errors.Add(new ParseError(token.Start, token.Length, code, message));
    }

    public ParseResult Error(Token token, string code, string message)
    {
        AddError(token, code, message);
        return Fail();
    }

    public ParseResult Fail()
    {
        return ParseResult.Fail(_errors!, BuildAnnotatedQuery());
    }

    private string BuildAnnotatedQuery()
    {
        if (_errors is null || _errors.Count == 0)
            return _input;

        var sb = new StringBuilder(_input.Length + _errors.Count * 40);
        var lastPos = 0;

        foreach (var error in _errors)
        {
            var errorEnd = error.Position + error.Length;

            if (errorEnd > lastPos)
            {
                sb.Append(_input, lastPos, errorEnd - lastPos);
                lastPos = errorEnd;
            }

            sb.Append(" ##");
            sb.Append(error.Message);
            sb.Append("##");
        }

        if (lastPos < _input.Length)
            sb.Append(_input, lastPos, _input.Length - lastPos);

        return sb.ToString();
    }
}

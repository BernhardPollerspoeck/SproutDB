using System.Collections;
using System.Globalization;
using System.Text;

namespace SproutDB.Core.Parsing;

/// <summary>
/// Renders CLR values as SproutDB query literals — the one place that turns
/// values into query text (parameters, typed LINQ API).
/// </summary>
internal static class QueryLiteral
{
    /// <summary>
    /// Formats <paramref name="value"/> as a literal, or returns false with a reason
    /// if the value cannot be represented safely.
    /// </summary>
    public static bool TryFormat(object? value, out string literal, out string? error)
    {
        error = null;
        literal = value switch
        {
            null => "null",
            string s => Quote(s),
            char ch => Quote(ch.ToString()),
            bool b => b ? "true" : "false",
            sbyte v => v.ToString(CultureInfo.InvariantCulture),
            byte v => v.ToString(CultureInfo.InvariantCulture),
            short v => v.ToString(CultureInfo.InvariantCulture),
            ushort v => v.ToString(CultureInfo.InvariantCulture),
            int v => v.ToString(CultureInfo.InvariantCulture),
            uint v => v.ToString(CultureInfo.InvariantCulture),
            long v => v.ToString(CultureInfo.InvariantCulture),
            ulong v => v.ToString(CultureInfo.InvariantCulture),
            decimal v => v.ToString(CultureInfo.InvariantCulture),
            float v => FormatFloating(v),
            double v => FormatFloating(v),
            DateOnly d => Quote(d.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture)),
            TimeOnly t => Quote(t.ToString("HH:mm:ss", CultureInfo.InvariantCulture)),
            DateTime dt => Quote(dt.ToString("yyyy-MM-dd HH:mm:ss", CultureInfo.InvariantCulture)),
            DateTimeOffset dto => Quote(dto.UtcDateTime.ToString("yyyy-MM-dd HH:mm:ss", CultureInfo.InvariantCulture)),
            byte[] bytes => Quote(Convert.ToBase64String(bytes)),
            IEnumerable items => FormatList(items),
            _ => Quote(value.ToString() ?? ""),
        };

        if (literal.Length == 0)
        {
            error = value is float or double
                ? "NaN and infinity cannot be stored"
                : "lists may only contain single values";
            return false;
        }

        if (!IsSingleValue(literal))
        {
            error = "value could not be rendered as a literal";
            literal = "";
            return false;
        }

        return true;
    }

    private static string Quote(string s) => $"'{StringLiteral.Escape(s)}'";

    private static string FormatFloating(double v)
    {
        if (double.IsNaN(v) || double.IsInfinity(v))
            return "";

        // The tokenizer has no exponent notation — spell out small/large values
        var s = v.ToString("R", CultureInfo.InvariantCulture);
        return s.Contains('E')
            ? v.ToString("0.############################", CultureInfo.InvariantCulture)
            : s;
    }

    private static string FormatList(IEnumerable items)
    {
        var sb = new StringBuilder("[");
        var first = true;
        foreach (var item in items)
        {
            // No nested lists (a string is IEnumerable too, but handled above)
            if (item is IEnumerable and not string and not byte[])
                return "";
            if (!TryFormat(item, out var element, out _))
                return "";
            if (!first) sb.Append(", ");
            first = false;
            sb.Append(element);
        }
        sb.Append(']');
        return sb.ToString();
    }

    /// <summary>
    /// Safety net for the escaping: the rendered text must tokenize into exactly one
    /// value — a scalar literal (optionally negative) or a bracketed list of them.
    /// Never an identifier, operator or statement delimiter.
    /// </summary>
    private static bool IsSingleValue(string literal)
    {
        var tokens = Tokenizer.Tokenize(literal);
        var pos = 0;

        if (tokens[pos].Type == TokenType.LeftBracket)
        {
            pos++;
            if (tokens[pos].Type != TokenType.RightBracket)
            {
                while (true)
                {
                    if (!TryScalar(literal, tokens, ref pos))
                        return false;
                    if (tokens[pos].Type == TokenType.Comma)
                    {
                        pos++;
                        continue;
                    }
                    break;
                }
            }
            if (tokens[pos].Type != TokenType.RightBracket)
                return false;
            pos++;
        }
        else if (!TryScalar(literal, tokens, ref pos))
        {
            return false;
        }

        return tokens[pos].Type == TokenType.Eof;
    }

    private static bool TryScalar(string literal, List<Token> tokens, ref int pos)
    {
        var t = tokens[pos];
        switch (t.Type)
        {
            case TokenType.StringLiteral:
                // Must be closed by an unescaped quote
                if (t.Length < 2 || literal[t.Start + t.Length - 1] != '\'')
                    return false;
                pos++;
                return true;

            case TokenType.IntegerLiteral:
            case TokenType.FloatLiteral:
                pos++;
                return true;

            case TokenType.Minus:
                if (tokens[pos + 1].Type is not (TokenType.IntegerLiteral or TokenType.FloatLiteral))
                    return false;
                pos += 2;
                return true;

            case TokenType.Identifier:
                var word = literal.AsSpan(t.Start, t.Length);
                if (!word.SequenceEqual("true") && !word.SequenceEqual("false") && !word.SequenceEqual("null"))
                    return false;
                pos++;
                return true;

            default:
                return false;
        }
    }
}

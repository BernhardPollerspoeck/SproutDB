using System.Text;

namespace SproutDB.Core.Parsing;

/// <summary>
/// Escape rules for single-quoted string literals.
/// Two escapes exist: <c>\'</c> → <c>'</c> and <c>\\</c> → <c>\</c>.
/// Any other backslash is kept literally (<c>'C:\temp'</c> stays <c>C:\temp</c>).
/// </summary>
internal static class StringLiteral
{
    /// <summary>
    /// Returns the unescaped content of a string literal token (quotes stripped).
    /// </summary>
    public static string Unescape(string input, Token token)
    {
        var content = input.AsSpan(token.Start + 1, token.Length - 2);
        if (content.IndexOf('\\') < 0)
            return content.ToString();

        var sb = new StringBuilder(content.Length);
        for (var i = 0; i < content.Length; i++)
        {
            var c = content[i];
            if (c == '\\' && i + 1 < content.Length && (content[i + 1] == '\\' || content[i + 1] == '\''))
            {
                sb.Append(content[i + 1]);
                i++;
                continue;
            }
            sb.Append(c);
        }
        return sb.ToString();
    }

    /// <summary>
    /// Escapes a value for embedding between single quotes in a query.
    /// Backslashes first, then quotes — otherwise the added backslashes would be doubled.
    /// </summary>
    public static string Escape(string value) => value.Replace("\\", "\\\\").Replace("'", "\\'");
}

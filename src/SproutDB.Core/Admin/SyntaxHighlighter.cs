using System.Text;
using Microsoft.AspNetCore.Components;

namespace SproutDB.Core.Admin;

/// <summary>
/// Context-aware syntax highlighter for SproutDB queries.
/// Tracks what comes after keywords to colorize table names and field names.
/// </summary>
internal static class SyntaxHighlighter
{
    private static readonly HashSet<string> Commands = new(StringComparer.OrdinalIgnoreCase)
    {
        "get", "upsert", "delete", "describe", "create", "purge",
        "add", "alter", "rename", "backup", "restore", "shrink",
        "grant", "revoke", "restrict", "unrestrict", "rotate",
    };

    private static readonly HashSet<string> Clauses = new(StringComparer.OrdinalIgnoreCase)
    {
        "from", "where", "order", "by", "limit", "offset", "as",
        "select", "follow", "page", "database", "table", "column",
        "index", "on", "to", "apikey", "set",
    };

    private static readonly HashSet<string> Operators = new(StringComparer.OrdinalIgnoreCase)
    {
        "and", "or", "not", "in", "like", "between",
        "contains", "starts", "ends", "startswith", "endswith", "is",
    };

    private static readonly HashSet<string> Aggregates = new(StringComparer.OrdinalIgnoreCase)
    {
        "count", "sum", "avg", "min", "max", "group",
    };

    private static readonly HashSet<string> Booleans = new(StringComparer.OrdinalIgnoreCase)
    {
        "true", "false", "null",
    };

    // After these keywords, next identifier = table name
    private static readonly HashSet<string> TablePrecursors = new(StringComparer.OrdinalIgnoreCase)
    {
        "get", "upsert", "delete", "describe", "purge",
        "from", "table",
    };

    // After these keywords, identifiers = field names
    private static readonly HashSet<string> FieldPrecursors = new(StringComparer.OrdinalIgnoreCase)
    {
        "where", "select", "by",
    };

    private enum Context { Normal, ExpectTable, InFields, InObject }

    public static MarkupString Highlight(string? query)
    {
        if (string.IsNullOrEmpty(query))
            return new MarkupString(string.Empty);

        var sb = new StringBuilder(query.Length * 2);
        int i = 0;
        int len = query.Length;
        var ctx = Context.Normal;
        int braceDepth = 0;
        bool objectBeforeColon = false;

        while (i < len)
        {
            char c = query[i];

            // Newline resets context for multi-query
            if (c == '\n')
            {
                sb.Append(c);
                i++;
                ctx = Context.Normal;
                braceDepth = 0;
                continue;
            }

            // Comments: ## ... ##
            if (c == '#' && i + 1 < len && query[i + 1] == '#')
            {
                int start = i;
                i += 2;
                while (i < len)
                {
                    if (query[i] == '#' && i + 1 < len && query[i + 1] == '#')
                    {
                        i += 2;
                        break;
                    }
                    if (query[i] == '\n') break;
                    i++;
                }
                sb.Append("<span class=\"hl-comment\">");
                AppendEscaped(sb, query, start, i - start);
                sb.Append("</span>");
                continue;
            }

            // Strings: '...' with \' escape
            if (c == '\'')
            {
                int start = i;
                i++;
                while (i < len)
                {
                    if (query[i] == '\\' && i + 1 < len)
                    {
                        i += 2;
                        continue;
                    }
                    if (query[i] == '\'')
                    {
                        i++;
                        break;
                    }
                    i++;
                }
                sb.Append("<span class=\"hl-string\">");
                AppendEscaped(sb, query, start, i - start);
                sb.Append("</span>");
                // After a value, we're not expecting table anymore
                if (ctx == Context.ExpectTable) ctx = Context.Normal;
                continue;
            }

            // Numbers
            if (char.IsDigit(c) || (c == '-' && i + 1 < len && char.IsDigit(query[i + 1]) && (i == 0 || !char.IsLetterOrDigit(query[i - 1]))))
            {
                int start = i;
                if (c == '-') i++;
                while (i < len && (char.IsDigit(query[i]) || query[i] == '.'))
                    i++;
                sb.Append("<span class=\"hl-number\">");
                AppendEscaped(sb, query, start, i - start);
                sb.Append("</span>");
                if (ctx == Context.ExpectTable) ctx = Context.Normal;
                continue;
            }

            // Braces — track object context
            if (c == '{')
            {
                braceDepth++;
                objectBeforeColon = true;
                sb.Append("<span class=\"hl-symbol\">");
                AppendEscapedChar(sb, c);
                sb.Append("</span>");
                i++;
                continue;
            }

            if (c == '}')
            {
                braceDepth--;
                if (braceDepth <= 0)
                {
                    braceDepth = 0;
                    objectBeforeColon = false;
                }
                sb.Append("<span class=\"hl-symbol\">");
                AppendEscapedChar(sb, c);
                sb.Append("</span>");
                i++;
                continue;
            }

            // Colon inside object — switches from key to value
            if (c == ':' && braceDepth > 0)
            {
                objectBeforeColon = false;
                sb.Append("<span class=\"hl-symbol\">");
                AppendEscapedChar(sb, c);
                sb.Append("</span>");
                i++;
                continue;
            }

            // Comma inside object — back to key position
            if (c == ',' && braceDepth > 0)
            {
                objectBeforeColon = true;
                sb.Append("<span class=\"hl-symbol\">");
                AppendEscapedChar(sb, c);
                sb.Append("</span>");
                i++;
                continue;
            }

            // Identifiers / keywords
            if (char.IsLetter(c) || c == '_')
            {
                int start = i;
                while (i < len && (char.IsLetterOrDigit(query[i]) || query[i] == '_'))
                    i++;

                string word = query.Substring(start, i - start);

                // Classify keyword first
                if (Commands.Contains(word))
                {
                    AppendSpan(sb, "command", query, start, i - start);
                    ctx = Context.ExpectTable;
                }
                else if (Clauses.Contains(word))
                {
                    AppendSpan(sb, "clause", query, start, i - start);
                    if (TablePrecursors.Contains(word))
                        ctx = Context.ExpectTable;
                    else if (FieldPrecursors.Contains(word))
                        ctx = Context.InFields;
                }
                else if (Operators.Contains(word))
                {
                    AppendSpan(sb, "operator", query, start, i - start);
                }
                else if (Aggregates.Contains(word))
                {
                    AppendSpan(sb, "aggregate", query, start, i - start);
                    // After aggregate function name, args are fields
                }
                else if (Booleans.Contains(word))
                {
                    AppendSpan(sb, "boolean", query, start, i - start);
                }
                else
                {
                    // Unknown identifier — classify by context
                    if (braceDepth > 0 && objectBeforeColon)
                    {
                        AppendSpan(sb, "field", query, start, i - start);
                    }
                    else if (ctx == Context.ExpectTable)
                    {
                        AppendSpan(sb, "table", query, start, i - start);
                        ctx = Context.Normal;
                    }
                    else if (ctx == Context.InFields)
                    {
                        AppendSpan(sb, "field", query, start, i - start);
                    }
                    else
                    {
                        // Default — still give it field color if nothing else matches
                        AppendSpan(sb, "ident", query, start, i - start);
                    }
                }
                continue;
            }

            // Parentheses
            if (c == '(' || c == ')')
            {
                sb.Append("<span class=\"hl-symbol\">");
                AppendEscapedChar(sb, c);
                sb.Append("</span>");
                i++;
                continue;
            }

            // Other symbols: , = != < > <= >= *
            if (IsSymbol(c))
            {
                sb.Append("<span class=\"hl-symbol\">");
                AppendEscapedChar(sb, c);
                sb.Append("</span>");
                i++;
                continue;
            }

            // Whitespace and everything else
            AppendEscapedChar(sb, c);
            i++;
        }

        return new MarkupString(sb.ToString());
    }

    private static void AppendSpan(StringBuilder sb, string cls, string text, int start, int length)
    {
        sb.Append("<span class=\"hl-");
        sb.Append(cls);
        sb.Append("\">");
        AppendEscaped(sb, text, start, length);
        sb.Append("</span>");
    }

    private static bool IsSymbol(char c) =>
        c is ',' or ':' or '=' or '<' or '>' or '*' or '!';

    private static void AppendEscaped(StringBuilder sb, string text, int start, int length)
    {
        int end = start + length;
        for (int i = start; i < end; i++)
            AppendEscapedChar(sb, text[i]);
    }

    private static void AppendEscapedChar(StringBuilder sb, char c)
    {
        switch (c)
        {
            case '<': sb.Append("&lt;"); break;
            case '>': sb.Append("&gt;"); break;
            case '&': sb.Append("&amp;"); break;
            case '"': sb.Append("&quot;"); break;
            default: sb.Append(c); break;
        }
    }
}

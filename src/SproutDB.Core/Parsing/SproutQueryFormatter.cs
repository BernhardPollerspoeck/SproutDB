using System.Text;

namespace SproutDB.Core.Parsing;

public static class SproutQueryFormatter
{
    private static readonly HashSet<string> GetClauses = new(StringComparer.OrdinalIgnoreCase)
    {
        "where", "select", "order", "group", "limit", "page",
        "follow", "distinct", "count", "sum", "avg", "min", "max",
    };

    private static readonly HashSet<string> Aggregates = new(StringComparer.OrdinalIgnoreCase)
    {
        "sum", "avg", "min", "max", "count",
    };

    public static string Format(string query)
    {
        if (string.IsNullOrWhiteSpace(query))
            return query;

        var tokens = Tokenizer.Tokenize(query);
        if (tokens.Count <= 1)
            return query;

        var input = query.AsSpan();
        var command = tokens[0].GetText(input).ToString().ToLowerInvariant();

        return command switch
        {
            "get" => FormatGet(tokens, input),
            "upsert" => FormatUpsert(tokens, input),
            "delete" => FormatDelete(tokens, input),
            _ => FormatSimple(tokens, input),
        };
    }

    private static string FormatGet(List<Token> tokens, ReadOnlySpan<char> input)
    {
        var sb = new StringBuilder();
        var i = 0;
        var indent = "    ";
        var followDepth = 0;

        // "get <table>" on first line
        sb.Append(TokenText(tokens, i, input)); // get
        i++;
        if (i < tokens.Count - 1 && tokens[i].Type == TokenType.Identifier)
        {
            sb.Append(' ');
            sb.Append(TokenText(tokens, i, input)); // table
            i++;
        }

        // Handle -select (Minus + select)
        // Track if we're inside a where clause for and/or handling
        var inWhere = false;
        var inFollowWhere = false;

        while (i < tokens.Count && tokens[i].Type != TokenType.Eof)
        {
            var text = TokenText(tokens, i, input);
            var lower = text.ToLowerInvariant();

            // -select pattern: Minus followed by "select"
            if (tokens[i].Type == TokenType.Minus && i + 1 < tokens.Count - 1)
            {
                var nextText = TokenText(tokens, i + 1, input);
                if (nextText.Equals("select", StringComparison.OrdinalIgnoreCase))
                {
                    inWhere = false;
                    inFollowWhere = false;
                    sb.Append('\n');
                    sb.Append(GetIndent(indent, followDepth));
                    sb.Append('-');
                    i++;
                    sb.Append(TokenText(tokens, i, input)); // select
                    i++;
                    // Collect columns
                    AppendColumnsUntilClause(sb, tokens, ref i, input);
                    continue;
                }
            }

            if (tokens[i].Type != TokenType.Identifier)
            {
                // No space before/after dots (table.column)
                if (tokens[i].Type == TokenType.Dot || (i > 0 && tokens[i - 1].Type == TokenType.Dot))
                    sb.Append(text);
                else
                {
                    sb.Append(' ');
                    sb.Append(text);
                }
                i++;
                continue;
            }

            // No space after dot (table.column)
            if (i > 0 && tokens[i - 1].Type == TokenType.Dot)
            {
                sb.Append(text);
                i++;
                continue;
            }

            // and/or inside where
            if ((lower == "and" || lower == "or") && (inWhere || inFollowWhere))
            {
                sb.Append('\n');
                var whereIndent = inFollowWhere
                    ? GetIndent(indent, followDepth + 1) + "  "
                    : indent + "  ";
                sb.Append(whereIndent);
                sb.Append(lower);
                i++;
                continue;
            }

            // follow clause
            if (lower == "follow")
            {
                inWhere = false;
                inFollowWhere = false;
                sb.Append('\n');
                sb.Append(GetIndent(indent, followDepth));
                sb.Append(lower);
                i++;

                // Collect tokens until next follow, sub-select, or sub-where
                while (i < tokens.Count && tokens[i].Type != TokenType.Eof)
                {
                    var ft = TokenText(tokens, i, input);
                    var fl = ft.ToLowerInvariant();

                    if (fl == "select")
                    {
                        sb.Append('\n');
                        sb.Append(GetIndent(indent, followDepth + 1));
                        sb.Append(fl);
                        i++;
                        // Collect select columns until where/follow/clause
                        AppendColumnsUntilFollowClause(sb, tokens, ref i, input);
                        // Continue to check for where after select
                        continue;
                    }

                    if (fl == "where")
                    {
                        inFollowWhere = true;
                        sb.Append('\n');
                        sb.Append(GetIndent(indent, followDepth + 1));
                        sb.Append(fl);
                        i++;
                        break;
                    }

                    if (fl == "follow" || (IsGetClause(fl) && fl != "as" && fl != "on"))
                        break;

                    // No space around dots
                    if (tokens[i].Type == TokenType.Dot || (i > 0 && tokens[i - 1].Type == TokenType.Dot))
                        sb.Append(ft);
                    else
                    {
                        sb.Append(' ');
                        sb.Append(ft);
                    }
                    i++;
                }
                continue;
            }

            // Regular clause keywords
            if (IsGetClause(lower))
            {
                var wasInWhere = inWhere;
                inWhere = lower == "where";
                inFollowWhere = false;

                // "by" after "order" or "group" stays on same line
                if (lower == "by")
                {
                    sb.Append(' ');
                    sb.Append(lower);
                    i++;
                    continue;
                }

                // "size" after "page N" stays on same line
                if (lower == "size")
                {
                    sb.Append(' ');
                    sb.Append(lower);
                    i++;
                    continue;
                }

                // "as" stays on same line
                if (lower == "as")
                {
                    sb.Append(' ');
                    sb.Append(lower);
                    i++;
                    continue;
                }

                // "on" stays on same line
                if (lower == "on")
                {
                    sb.Append(' ');
                    sb.Append(lower);
                    i++;
                    continue;
                }

                sb.Append('\n');
                sb.Append(GetIndent(indent, followDepth));
                sb.Append(lower);
                i++;
                continue;
            }

            // Regular token
            sb.Append(' ');
            sb.Append(text);
            i++;
        }

        return sb.ToString();
    }

    private static string FormatDelete(List<Token> tokens, ReadOnlySpan<char> input)
    {
        var sb = new StringBuilder();
        var i = 0;
        var indent = "    ";

        // "delete <table>"
        sb.Append(TokenText(tokens, i, input)); // delete
        i++;
        if (i < tokens.Count - 1 && tokens[i].Type == TokenType.Identifier)
        {
            sb.Append(' ');
            sb.Append(TokenText(tokens, i, input)); // table
            i++;
        }

        var inWhere = false;

        while (i < tokens.Count && tokens[i].Type != TokenType.Eof)
        {
            var text = TokenText(tokens, i, input);
            var lower = text.ToLowerInvariant();

            if (tokens[i].Type == TokenType.Identifier)
            {
                if ((lower == "and" || lower == "or") && inWhere)
                {
                    sb.Append('\n');
                    sb.Append(indent + "  ");
                    sb.Append(lower);
                    i++;
                    continue;
                }

                if (lower == "where")
                {
                    inWhere = true;
                    sb.Append('\n');
                    sb.Append(indent);
                    sb.Append(lower);
                    i++;
                    continue;
                }
            }

            AppendWithDotAwareness(sb, tokens, i, text);
            i++;
        }

        return sb.ToString();
    }

    private static string FormatUpsert(List<Token> tokens, ReadOnlySpan<char> input)
    {
        var sb = new StringBuilder();
        var i = 0;
        var indent = "    ";

        // "upsert <table>"
        sb.Append(TokenText(tokens, i, input)); // upsert
        i++;
        if (i < tokens.Count - 1 && tokens[i].Type == TokenType.Identifier)
        {
            sb.Append(' ');
            sb.Append(TokenText(tokens, i, input)); // table
            i++;
        }

        if (i >= tokens.Count - 1)
            return sb.ToString();

        // Detect bulk vs single
        var isBulk = tokens[i].Type == TokenType.LeftBracket;

        if (isBulk)
        {
            // Bulk: keep compact — just space-separated
            while (i < tokens.Count && tokens[i].Type != TokenType.Eof)
            {
                var text = TokenText(tokens, i, input);
                var lower = text.ToLowerInvariant();

                // "on" after closing bracket → new line
                if (tokens[i].Type == TokenType.Identifier && lower == "on")
                {
                    sb.Append('\n');
                    sb.Append(indent);
                    sb.Append(lower);
                    i++;
                    continue;
                }

                sb.Append(' ');
                sb.Append(text);
                i++;
            }
        }
        else if (tokens[i].Type == TokenType.LeftBrace)
        {
            // Single record: multi-line if >1 field
            var fields = CountFields(tokens, i);

            if (fields <= 1)
            {
                // Keep compact
                while (i < tokens.Count && tokens[i].Type != TokenType.Eof)
                {
                    var text = TokenText(tokens, i, input);
                    var lower = text.ToLowerInvariant();

                    if (tokens[i].Type == TokenType.Identifier && lower == "on"
                        && !IsInsideBraces(tokens, i))
                    {
                        sb.Append('\n');
                        sb.Append(indent);
                        sb.Append(lower);
                        i++;
                        continue;
                    }

                    sb.Append(' ');
                    sb.Append(text);
                    i++;
                }
            }
            else
            {
                // Multi-line: each field on new line
                sb.Append(" {");
                i++; // skip {
                var afterBrace = false;

                while (i < tokens.Count && tokens[i].Type != TokenType.Eof)
                {
                    if (tokens[i].Type == TokenType.RightBrace)
                    {
                        sb.Append('\n');
                        sb.Append('}');
                        i++;
                        afterBrace = true;
                        continue;
                    }

                    if (afterBrace)
                    {
                        var text = TokenText(tokens, i, input);
                        var lower = text.ToLowerInvariant();
                        if (tokens[i].Type == TokenType.Identifier && lower == "on")
                        {
                            sb.Append('\n');
                            sb.Append(indent);
                            sb.Append(lower);
                            i++;
                            continue;
                        }
                        sb.Append(' ');
                        sb.Append(text);
                        i++;
                        continue;
                    }

                    // Comma separates fields — next field on new line
                    if (tokens[i].Type == TokenType.Comma)
                    {
                        sb.Append(',');
                        i++;
                        continue;
                    }

                    // Field name (identifier followed by colon)
                    if (tokens[i].Type == TokenType.Identifier
                        && i + 1 < tokens.Count && tokens[i + 1].Type == TokenType.Colon)
                    {
                        sb.Append('\n');
                        sb.Append(indent);
                        sb.Append(TokenText(tokens, i, input)); // key
                        i++;
                        sb.Append(": ");
                        i++; // skip colon

                        // Value (can be multiple tokens for negative numbers etc.)
                        while (i < tokens.Count
                               && tokens[i].Type != TokenType.Comma
                               && tokens[i].Type != TokenType.RightBrace
                               && tokens[i].Type != TokenType.Eof)
                        {
                            sb.Append(TokenText(tokens, i, input));
                            i++;
                        }
                        continue;
                    }

                    // Fallback
                    sb.Append(' ');
                    sb.Append(TokenText(tokens, i, input));
                    i++;
                }
            }
        }
        else
        {
            // No body — just append remaining
            while (i < tokens.Count && tokens[i].Type != TokenType.Eof)
            {
                sb.Append(' ');
                sb.Append(TokenText(tokens, i, input));
                i++;
            }
        }

        return sb.ToString();
    }

    private static string FormatSimple(List<Token> tokens, ReadOnlySpan<char> input)
    {
        var sb = new StringBuilder();
        for (var i = 0; i < tokens.Count; i++)
        {
            if (tokens[i].Type == TokenType.Eof) break;
            if (i > 0)
                AppendWithDotAwareness(sb, tokens, i, TokenText(tokens, i, input));
            else
                sb.Append(TokenText(tokens, i, input));
        }
        return sb.ToString();
    }

    private static string TokenText(List<Token> tokens, int index, ReadOnlySpan<char> input)
        => tokens[index].GetText(input).ToString();

    private static void AppendWithDotAwareness(StringBuilder sb, List<Token> tokens, int i, string text)
    {
        if (tokens[i].Type == TokenType.Dot || (i > 0 && tokens[i - 1].Type == TokenType.Dot))
            sb.Append(text);
        else
        {
            sb.Append(' ');
            sb.Append(text);
        }
    }

    private static string GetIndent(string baseIndent, int depth)
    {
        if (depth <= 0) return baseIndent;
        var sb = new StringBuilder();
        for (var i = 0; i <= depth; i++)
            sb.Append(baseIndent);
        return sb.ToString();
    }

    private static bool IsGetClause(string lower)
        => GetClauses.Contains(lower) || lower is "by" or "as" or "size" or "on";

    private static void AppendColumnsUntilClause(StringBuilder sb, List<Token> tokens, ref int i, ReadOnlySpan<char> input)
    {
        while (i < tokens.Count && tokens[i].Type != TokenType.Eof)
        {
            var text = TokenText(tokens, i, input);
            if (tokens[i].Type == TokenType.Identifier && IsGetClause(text.ToLowerInvariant()))
                break;
            AppendWithDotAwareness(sb, tokens, i, text);
            i++;
        }
    }

    private static void AppendColumnsUntilFollowClause(StringBuilder sb, List<Token> tokens, ref int i, ReadOnlySpan<char> input)
    {
        while (i < tokens.Count && tokens[i].Type != TokenType.Eof)
        {
            var text = TokenText(tokens, i, input);
            if (tokens[i].Type == TokenType.Identifier)
            {
                var lower = text.ToLowerInvariant();
                if (lower is "where" or "follow" || IsGetClause(lower))
                    break;
            }
            AppendWithDotAwareness(sb, tokens, i, text);
            i++;
        }
    }

    private static int CountFields(List<Token> tokens, int braceStart)
    {
        var count = 0;
        var depth = 0;
        for (var i = braceStart; i < tokens.Count; i++)
        {
            if (tokens[i].Type == TokenType.LeftBrace) depth++;
            else if (tokens[i].Type == TokenType.RightBrace) { depth--; if (depth == 0) break; }
            else if (tokens[i].Type == TokenType.Colon && depth == 1) count++;
        }
        return count;
    }

    private static bool IsInsideBraces(List<Token> tokens, int index)
    {
        var depth = 0;
        for (var i = 0; i < index; i++)
        {
            if (tokens[i].Type == TokenType.LeftBrace) depth++;
            else if (tokens[i].Type == TokenType.RightBrace) depth--;
        }
        return depth > 0;
    }
}

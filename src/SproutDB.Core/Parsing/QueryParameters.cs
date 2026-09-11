using System.Collections;
using System.Reflection;
using System.Text;
using SproutDB.Core.Execution;

namespace SproutDB.Core.Parsing;

/// <summary>
/// Binds <c>@name</c> placeholders: each one is replaced by exactly one literal
/// token (see <see cref="QueryLiteral"/>) before the query is parsed. The rendered
/// query is what runs and what the WAL stores, so live execution and replay parse
/// the very same text. Values can never become query syntax — that is the point.
/// </summary>
internal static class QueryParameters
{
    /// <summary>
    /// Replaces all placeholders. Fails with <c>PARAMETER_ERROR</c> for a missing,
    /// unused or unrenderable parameter. Without placeholders and parameters the
    /// query is returned unchanged.
    /// </summary>
    public static bool TryBind(string query, IReadOnlyDictionary<string, object?>? parameters,
        out string bound, out SproutResponse? error)
    {
        bound = query;
        error = null;

        var hasParameters = parameters is { Count: > 0 };
        if (!hasParameters && query.IndexOf('@') < 0)
            return true;

        // Placeholder names are case-insensitive, like all SproutDB identifiers
        var lookup = new Dictionary<string, object?>(StringComparer.OrdinalIgnoreCase);
        List<SproutError>? errors = null;
        if (parameters is not null)
        {
            foreach (var (name, value) in parameters)
            {
                var key = name.StartsWith('@') ? name[1..] : name;
                if (!lookup.TryAdd(key, value))
                    AddError(ref errors, $"parameter '@{key}' is given more than once (names are case-insensitive)");
            }
        }

        var tokens = Tokenizer.Tokenize(query);
        var used = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        var sb = new StringBuilder(query.Length + 32);
        var copied = 0;

        foreach (var token in tokens)
        {
            if (token.Type != TokenType.Parameter)
                continue;

            var name = query.Substring(token.Start + 1, token.Length - 1);
            if (!lookup.TryGetValue(name, out var value))
            {
                AddError(ref errors, $"missing parameter '@{name}'", token);
                continue;
            }

            used.Add(name);
            if (!QueryLiteral.TryFormat(value, out var literal, out var reason))
            {
                AddError(ref errors, $"parameter '@{name}': {reason}", token);
                continue;
            }

            sb.Append(query, copied, token.Start - copied);
            sb.Append(literal);
            copied = token.Start + token.Length;
        }

        foreach (var name in lookup.Keys)
        {
            if (!used.Contains(name))
                AddError(ref errors, $"unused parameter '@{name}'");
        }

        if (errors is not null)
        {
            error = ResponseHelper.Errors(query, errors);
            return false;
        }

        sb.Append(query, copied, query.Length - copied);
        bound = sb.ToString();
        return true;
    }

    /// <summary>
    /// Reads parameters from an anonymous/POCO object (public properties) or a
    /// dictionary with string keys.
    /// </summary>
    public static IReadOnlyDictionary<string, object?> FromObject(object parameters)
    {
        switch (parameters)
        {
            case IReadOnlyDictionary<string, object?> typed:
                return typed;

            case IDictionary dictionary:
            {
                var result = new Dictionary<string, object?>(dictionary.Count);
                foreach (DictionaryEntry entry in dictionary)
                {
                    if (entry.Key is string key)
                        result[key] = entry.Value;
                    else
                        throw new ArgumentException("parameter dictionary keys must be strings", nameof(parameters));
                }
                return result;
            }

            default:
            {
                var result = new Dictionary<string, object?>();
                foreach (var prop in parameters.GetType().GetProperties(BindingFlags.Public | BindingFlags.Instance))
                {
                    if (prop.CanRead && prop.GetIndexParameters().Length == 0)
                        result[prop.Name] = prop.GetValue(parameters);
                }
                return result;
            }
        }
    }

    private static void AddError(ref List<SproutError>? errors, string message, Token? token = null)
    {
        errors ??= [];
        errors.Add(new SproutError
        {
            Code = ErrorCodes.PARAMETER_ERROR,
            Message = message,
            Position = token?.Start ?? -1,
            Length = token?.Length ?? 0,
        });
    }
}

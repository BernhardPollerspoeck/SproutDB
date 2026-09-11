using SproutDB.Core.Parsing;

namespace SproutDB.Core.Linq;

/// <summary>
/// Collects values while the typed API builds a query: each value becomes a
/// placeholder (<c>@p0</c>, <c>@p1</c>, …) that is bound when the query runs.
/// Without a collector, values are inlined as literals (same rendering).
/// </summary>
internal sealed class ParameterCollector
{
    private readonly Dictionary<string, object?> _values = [];

    public IReadOnlyDictionary<string, object?> Values => _values;

    public string Add(object? value)
    {
        var name = $"p{_values.Count}";
        _values[name] = value;
        return "@" + name;
    }

    /// <summary>
    /// Placeholder via <paramref name="parameters"/>, or the inline literal when null.
    /// </summary>
    public static string Render(object? value, ParameterCollector? parameters)
    {
        if (parameters is not null)
            return parameters.Add(value);

        if (!QueryLiteral.TryFormat(value, out var literal, out var error))
            throw new SproutQueryException($"Unsupported value: {error}");
        return literal;
    }
}

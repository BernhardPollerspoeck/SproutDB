using System.Globalization;
using System.Reflection;
using System.Text;

namespace SproutDB.Core.Linq;

internal static class TypeMapper
{
    internal static string ToColumnName(string propertyName)
    {
        if (string.Equals(propertyName, "Id", StringComparison.Ordinal))
            return "_id";
        return propertyName.ToLowerInvariant();
    }

    internal static T Deserialize<T>(Dictionary<string, object?> row) where T : class, ISproutEntity, new()
    {
        var obj = new T();
        var properties = typeof(T).GetProperties(BindingFlags.Public | BindingFlags.Instance);

        foreach (var prop in properties)
        {
            if (!prop.CanWrite) continue;

            var colName = ToColumnName(prop.Name);
            if (!row.TryGetValue(colName, out var value))
                continue;

            if (value is null)
            {
                if (IsNullableType(prop.PropertyType))
                    prop.SetValue(obj, null);
                continue;
            }

            prop.SetValue(obj, ConvertValue(value, prop.PropertyType));
        }

        return obj;
    }

    /// <param name="parameters">
    /// Collects the values as placeholders; null inlines them as literals.
    /// </param>
    internal static string SerializeToUpsertFields(object obj, ParameterCollector? parameters = null)
    {
        var sb = new StringBuilder();
        sb.Append('{');
        var first = true;

        var properties = obj.GetType().GetProperties(BindingFlags.Public | BindingFlags.Instance);
        foreach (var prop in properties)
        {
            if (!prop.CanRead) continue;

            var colName = ToColumnName(prop.Name);
            var value = prop.GetValue(obj);

            // Skip _id when default (0) — let DB auto-assign
            if (colName == "_id" && value is ulong ulongVal && ulongVal == 0)
                continue;

            if (!first) sb.Append(", ");
            first = false;

            sb.Append(colName);
            sb.Append(": ");
            sb.Append(ParameterCollector.Render(value, parameters));
        }

        sb.Append('}');
        return sb.ToString();
    }

    private static object ConvertValue(object value, Type targetType)
    {
        var underlying = Nullable.GetUnderlyingType(targetType) ?? targetType;

        if (underlying == typeof(DateOnly) && value is string dateStr)
            return DateOnly.Parse(dateStr, CultureInfo.InvariantCulture);
        if (underlying == typeof(TimeOnly) && value is string timeStr)
            return TimeOnly.Parse(timeStr, CultureInfo.InvariantCulture);
        if (underlying == typeof(DateTime) && value is string dtStr)
            return DateTime.Parse(dtStr, CultureInfo.InvariantCulture);

        if (underlying.IsInstanceOfType(value))
            return value;

        return Convert.ChangeType(value, underlying, CultureInfo.InvariantCulture);
    }

    private static bool IsNullableType(Type type)
    {
        if (!type.IsValueType) return true;
        return Nullable.GetUnderlyingType(type) is not null;
    }
}

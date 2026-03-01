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

    internal static string SerializeToUpsertFields(object obj)
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
            sb.Append(FormatValue(value, prop.PropertyType));
        }

        sb.Append('}');
        return sb.ToString();
    }

    private static string FormatValue(object? value, Type propertyType)
    {
        if (value is null)
            return "null";

        var underlying = Nullable.GetUnderlyingType(propertyType) ?? propertyType;

        if (underlying == typeof(string)) return $"'{EscapeString((string)value)}'";
        if (underlying == typeof(bool)) return (bool)value ? "true" : "false";
        if (underlying == typeof(sbyte)) return ((sbyte)value).ToString(CultureInfo.InvariantCulture);
        if (underlying == typeof(byte)) return ((byte)value).ToString(CultureInfo.InvariantCulture);
        if (underlying == typeof(short)) return ((short)value).ToString(CultureInfo.InvariantCulture);
        if (underlying == typeof(ushort)) return ((ushort)value).ToString(CultureInfo.InvariantCulture);
        if (underlying == typeof(int)) return ((int)value).ToString(CultureInfo.InvariantCulture);
        if (underlying == typeof(uint)) return ((uint)value).ToString(CultureInfo.InvariantCulture);
        if (underlying == typeof(long)) return ((long)value).ToString(CultureInfo.InvariantCulture);
        if (underlying == typeof(ulong)) return ((ulong)value).ToString(CultureInfo.InvariantCulture);
        if (underlying == typeof(float)) return ((float)value).ToString(CultureInfo.InvariantCulture);
        if (underlying == typeof(double)) return ((double)value).ToString(CultureInfo.InvariantCulture);
        if (underlying == typeof(DateOnly)) return $"'{((DateOnly)value).ToString("yyyy-MM-dd")}'";
        if (underlying == typeof(TimeOnly)) return $"'{((TimeOnly)value).ToString("HH:mm:ss")}'";
        if (underlying == typeof(DateTime)) return $"'{((DateTime)value).ToString("yyyy-MM-dd HH:mm:ss")}'";

        return $"'{EscapeString(value.ToString() ?? "")}'";
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

    private static string EscapeString(string value)
    {
        return value.Replace("'", "\\'");
    }
}

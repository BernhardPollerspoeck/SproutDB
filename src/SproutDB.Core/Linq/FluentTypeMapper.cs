namespace SproutDB.Core.Linq;

/// <summary>
/// Maps CLR types to SproutDB type names for the fluent schema API.
/// </summary>
internal static class FluentTypeMapper
{
    /// <summary>
    /// Returns the SproutDB type name for the given CLR type.
    /// Throws <see cref="ArgumentException"/> for unsupported types.
    /// </summary>
    public static string GetTypeName(Type type)
    {
        var underlying = Nullable.GetUnderlyingType(type) ?? type;

        if (underlying == typeof(string)) return "string";
        if (underlying == typeof(bool)) return "bool";
        if (underlying == typeof(sbyte)) return "sbyte";
        if (underlying == typeof(byte)) return "ubyte";
        if (underlying == typeof(short)) return "sshort";
        if (underlying == typeof(ushort)) return "ushort";
        if (underlying == typeof(int)) return "sint";
        if (underlying == typeof(uint)) return "uint";
        if (underlying == typeof(long)) return "slong";
        if (underlying == typeof(ulong)) return "ulong";
        if (underlying == typeof(float)) return "float";
        if (underlying == typeof(double)) return "double";
        if (underlying == typeof(decimal)) return "double";
        if (underlying == typeof(DateTime)) return "datetime";
        if (underlying == typeof(DateOnly)) return "date";
        if (underlying == typeof(TimeOnly)) return "time";

        throw new ArgumentException($"Unsupported type '{underlying.Name}' for SproutDB column.");
    }

    /// <summary>
    /// Returns true if the given SproutDB type requires a size parameter.
    /// </summary>
    public static bool RequiresSize(string typeName) => typeName == "string";
}

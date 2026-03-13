namespace SproutDB.Core;

internal enum ColumnType : byte
{
    String,
    SByte,
    UByte,
    SShort,
    UShort,
    SInt,
    UInt,
    SLong,
    ULong,
    Float,
    Double,
    Bool,
    Date,
    Time,
    DateTime,
    Blob,
    Array,
}

internal static class ColumnTypes
{
    public const int DEFAULT_STRING_SIZE = 255;
    public const int MAX_STRING_SIZE = 1_048_576; // 1 MB

    public static int GetDefaultSize(ColumnType type) => type switch
    {
        ColumnType.String => DEFAULT_STRING_SIZE,
        ColumnType.SByte => 1,
        ColumnType.UByte => 1,
        ColumnType.Bool => 1,
        ColumnType.SShort => 2,
        ColumnType.UShort => 2,
        ColumnType.SInt => 4,
        ColumnType.UInt => 4,
        ColumnType.Float => 4,
        ColumnType.Date => 4,
        ColumnType.SLong => 8,
        ColumnType.ULong => 8,
        ColumnType.Double => 8,
        ColumnType.Time => 8,
        ColumnType.DateTime => 8,
        ColumnType.Blob => 8, // stores byte count as long
        ColumnType.Array => 8, // stores element count as long
        _ => throw new ArgumentOutOfRangeException(nameof(type)),
    };

    public static string GetName(ColumnType type) => type switch
    {
        ColumnType.String => "string",
        ColumnType.SByte => "sbyte",
        ColumnType.UByte => "ubyte",
        ColumnType.SShort => "sshort",
        ColumnType.UShort => "ushort",
        ColumnType.SInt => "sint",
        ColumnType.UInt => "uint",
        ColumnType.SLong => "slong",
        ColumnType.ULong => "ulong",
        ColumnType.Float => "float",
        ColumnType.Double => "double",
        ColumnType.Bool => "bool",
        ColumnType.Date => "date",
        ColumnType.Time => "time",
        ColumnType.DateTime => "datetime",
        ColumnType.Blob => "blob",
        ColumnType.Array => "array",
        _ => throw new ArgumentOutOfRangeException(nameof(type)),
    };

    public static bool TryParse(ReadOnlySpan<char> text, out ColumnType type)
    {
        type = default;
        return text.Length switch
        {
            4 => Try(text, "bool", ColumnType.Bool, ref type)
              || Try(text, "date", ColumnType.Date, ref type)
              || Try(text, "time", ColumnType.Time, ref type)
              || Try(text, "uint", ColumnType.UInt, ref type)
              || Try(text, "sint", ColumnType.SInt, ref type)
              || Try(text, "blob", ColumnType.Blob, ref type),

            5 => Try(text, "float", ColumnType.Float, ref type)
              || Try(text, "ubyte", ColumnType.UByte, ref type)
              || Try(text, "sbyte", ColumnType.SByte, ref type)
              || Try(text, "ulong", ColumnType.ULong, ref type)
              || Try(text, "slong", ColumnType.SLong, ref type)
              || Try(text, "array", ColumnType.Array, ref type),

            6 => Try(text, "string", ColumnType.String, ref type)
              || Try(text, "double", ColumnType.Double, ref type)
              || Try(text, "ushort", ColumnType.UShort, ref type)
              || Try(text, "sshort", ColumnType.SShort, ref type),

            8 => Try(text, "datetime", ColumnType.DateTime, ref type),

            _ => false,
        };
    }

    /// <summary>
    /// Checks if <paramref name="from"/> can be expanded to <paramref name="to"/>.
    /// Same type → true. Compatible widening → true. Otherwise false.
    /// </summary>
    public static bool CanExpand(ColumnType from, ColumnType to)
    {
        if (from == to) return true;

        return (from, to) switch
        {
            // Unsigned chain: ubyte → ushort → uint → ulong
            (ColumnType.UByte, ColumnType.UShort or ColumnType.UInt or ColumnType.ULong) => true,
            (ColumnType.UShort, ColumnType.UInt or ColumnType.ULong) => true,
            (ColumnType.UInt, ColumnType.ULong) => true,

            // Signed chain: sbyte → sshort → sint → slong
            (ColumnType.SByte, ColumnType.SShort or ColumnType.SInt or ColumnType.SLong) => true,
            (ColumnType.SShort, ColumnType.SInt or ColumnType.SLong) => true,
            (ColumnType.SInt, ColumnType.SLong) => true,

            // Unsigned → signed (only if MaxValue fits)
            // ubyte (255) → sshort/sint/slong ✅
            (ColumnType.UByte, ColumnType.SShort or ColumnType.SInt or ColumnType.SLong) => true,
            // ushort (65535) → sint/slong ✅
            (ColumnType.UShort, ColumnType.SInt or ColumnType.SLong) => true,
            // uint (4.2B) → slong ✅
            (ColumnType.UInt, ColumnType.SLong) => true,

            // Float → double
            (ColumnType.Float, ColumnType.Double) => true,

            _ => false,
        };
    }

    private static bool Try(ReadOnlySpan<char> text, string keyword, ColumnType value, ref ColumnType result)
    {
        if (!text.Equals(keyword, StringComparison.OrdinalIgnoreCase))
            return false;
        result = value;
        return true;
    }
}

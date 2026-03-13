using System.Text;

namespace SproutDB.Core.Storage;

/// <summary>
/// Binary schema file reader/writer for _schema.bin.
///
/// Layout:
///   [8 bytes] created_ticks (long, UTC)
///   [8 bytes] ttl_seconds (long, 0 = no TTL)
///   [2 bytes] column_count (ushort)
///   per column:
///     [1 byte]  name_length
///     [N bytes] name (UTF-8)
///     [1 byte]  column_type (ColumnType enum value)
///     [4 bytes] size (int)
///     [4 bytes] entry_size (int)
///     [1 byte]  flags (bit 0 = nullable, bit 1 = strict, bit 2 = unique)
///     [2 bytes] default_length (ushort, 0 = no default)
///     [N bytes] default (UTF-8, only if default_length > 0)
/// </summary>
internal static class SchemaFile
{
    private const byte FLAG_NULLABLE = 0x01;
    private const byte FLAG_STRICT = 0x02;
    private const byte FLAG_UNIQUE = 0x04;

    public static void Write(string path, TableSchema schema)
    {
        using var fs = new FileStream(path, FileMode.Create, FileAccess.Write, FileShare.None);
        using var bw = new BinaryWriter(fs, Encoding.UTF8);

        // Header
        bw.Write(schema.CreatedTicks);
        bw.Write(schema.TtlSeconds);
        bw.Write((ushort)schema.Columns.Count);

        // Columns
        foreach (var col in schema.Columns)
        {
            var nameBytes = Encoding.UTF8.GetBytes(col.Name);
            bw.Write((byte)nameBytes.Length);
            bw.Write(nameBytes);

            ColumnTypes.TryParse(col.Type, out var colType);
            bw.Write((byte)colType);
            bw.Write(col.Size);
            bw.Write(col.EntrySize);

            byte flags = 0;
            if (col.Nullable) flags |= FLAG_NULLABLE;
            if (col.Strict) flags |= FLAG_STRICT;
            if (col.IsUnique) flags |= FLAG_UNIQUE;
            bw.Write(flags);

            if (col.Default is not null)
            {
                var defaultBytes = Encoding.UTF8.GetBytes(col.Default);
                bw.Write((ushort)defaultBytes.Length);
                bw.Write(defaultBytes);
            }
            else
            {
                bw.Write((ushort)0);
            }

            // Array element type info (only for array columns)
            if (colType == ColumnType.Array)
            {
                ColumnTypes.TryParse(col.ElementType ?? "string", out var elemType);
                bw.Write((byte)elemType);
                bw.Write(col.ElementSize);
            }
        }
    }

    public static TableSchema Read(string path)
    {
        using var fs = new FileStream(path, FileMode.Open, FileAccess.Read, FileShare.Read);
        using var br = new BinaryReader(fs, Encoding.UTF8);

        var createdTicks = br.ReadInt64();
        var ttlSeconds = br.ReadInt64();
        var columnCount = br.ReadUInt16();

        var columns = new List<ColumnSchemaEntry>(columnCount);
        for (var i = 0; i < columnCount; i++)
        {
            var nameLen = br.ReadByte();
            var name = Encoding.UTF8.GetString(br.ReadBytes(nameLen));

            var colTypeByte = br.ReadByte();
            var colType = (ColumnType)colTypeByte;
            var size = br.ReadInt32();
            var entrySize = br.ReadInt32();
            var flags = br.ReadByte();

            var defaultLen = br.ReadUInt16();
            string? defaultValue = defaultLen > 0
                ? Encoding.UTF8.GetString(br.ReadBytes(defaultLen))
                : null;

            // Array element type info (only for array columns)
            string? elementType = null;
            int elementSize = 0;
            if (colType == ColumnType.Array)
            {
                var elemTypeByte = br.ReadByte();
                elementType = ColumnTypes.GetName((ColumnType)elemTypeByte);
                elementSize = br.ReadInt32();
            }

            columns.Add(new ColumnSchemaEntry
            {
                Name = name,
                Type = ColumnTypes.GetName(colType),
                Size = size,
                EntrySize = entrySize,
                Nullable = (flags & FLAG_NULLABLE) != 0,
                Strict = (flags & FLAG_STRICT) != 0,
                IsUnique = (flags & FLAG_UNIQUE) != 0,
                Default = defaultValue,
                ElementType = elementType,
                ElementSize = elementSize,
            });
        }

        return new TableSchema
        {
            CreatedTicks = createdTicks,
            TtlSeconds = ttlSeconds,
            Columns = columns,
        };
    }
}

using System.Buffers.Binary;
using System.Globalization;
using System.IO.MemoryMappedFiles;
using System.Text;
using SproutDB.Core.Parsing;

namespace SproutDB.Core.Storage;

internal sealed class ColumnHandle : IDisposable
{
    private readonly string _path;
    private readonly int _chunkSize;
    private FileStream _fs;
    private MemoryMappedFile _mmf;
    private volatile MemoryMappedViewAccessor _view;
    private long _capacity;

    public ColumnSchemaEntry Schema { get; }
    public ColumnType Type { get; }

    public ColumnHandle(string path, ColumnSchemaEntry schema, int chunkSize = StorageConstants.CHUNK_SIZE)
    {
        _path = path;
        _chunkSize = chunkSize;
        Schema = schema;
        ColumnTypes.TryParse(schema.Type, out var type);
        Type = type;

        _fs = new FileStream(path, FileMode.Open, FileAccess.ReadWrite, FileShare.ReadWrite);
        _capacity = _fs.Length;
        (_mmf, _view) = CreateMapping(_fs, _capacity);
    }

    // ── Write ───────────────────────────────────────────────

    public void WriteNull(long place)
    {
        EnsureCapacity(place + 1);
        var offset = place * Schema.EntrySize;

        // Flag = 0x00, zero the value bytes
        _view.Write(offset, StorageConstants.FLAG_NULL);
        var valueBuf = new byte[Schema.Size];
        _view.WriteArray(offset + 1, valueBuf, 0, valueBuf.Length);
    }

    public object WriteValue(long place, string raw)
    {
        EnsureCapacity(place + 1);
        var offset = place * Schema.EntrySize;

        _view.Write(offset, StorageConstants.FLAG_VALUE);
        return EncodeAndWrite(offset + 1, raw);
    }

    // ── Read ────────────────────────────────────────────────

    public object? ReadValue(long place)
    {
        var offset = place * Schema.EntrySize;
        if (offset + Schema.EntrySize > _capacity)
            return null;

        var flag = _view.ReadByte(offset);
        if (flag == StorageConstants.FLAG_NULL)
            return null;

        return ReadAndDecode(offset + 1);
    }

    // ── Match (zero-allocation per row) ────────────────────

    /// <summary>
    /// Encodes a raw value into the byte representation stored in the MMF.
    /// Call once before scanning; pass the result to <see cref="MatchesAtPlace"/>.
    /// </summary>
    public byte[] EncodeValueToBytes(string raw)
    {
        var buf = new byte[Schema.Size];
        EncodeIntoBuffer(buf, raw);
        return buf;
    }

    /// <summary>
    /// Returns true if the value at <paramref name="place"/> equals <paramref name="encoded"/>.
    /// Zero heap allocations — reads byte-by-byte from the MMF.
    /// </summary>
    public bool MatchesAtPlace(long place, byte[] encoded)
    {
        var offset = place * Schema.EntrySize;
        if (offset + Schema.EntrySize > _capacity)
            return false;

        if (_view.ReadByte(offset) != StorageConstants.FLAG_VALUE)
            return false;

        var valueOffset = offset + 1;
        for (int i = 0; i < encoded.Length; i++)
        {
            if (_view.ReadByte(valueOffset + i) != encoded[i])
                return false;
        }
        return true;
    }

    /// <summary>
    /// Returns true if the value at <paramref name="place"/> is null.
    /// </summary>
    public bool IsNullAtPlace(long place)
    {
        var offset = place * Schema.EntrySize;
        if (offset + Schema.EntrySize > _capacity)
            return true;
        return _view.ReadByte(offset) == StorageConstants.FLAG_NULL;
    }

    /// <summary>
    /// Compares the value at <paramref name="place"/> against <paramref name="encoded"/>.
    /// Returns negative if stored &lt; encoded, zero if equal, positive if stored &gt; encoded.
    /// Returns null if the stored value is null.
    /// </summary>
    public int? CompareAtPlace(long place, byte[] encoded)
    {
        var offset = place * Schema.EntrySize;
        if (offset + Schema.EntrySize > _capacity)
            return null;

        if (_view.ReadByte(offset) != StorageConstants.FLAG_VALUE)
            return null;

        var valueOffset = offset + 1;
        return CompareBytes(valueOffset, encoded);
    }

    private int CompareBytes(long offset, byte[] encoded)
    {
        return Type switch
        {
            ColumnType.Bool => _view.ReadByte(offset).CompareTo(encoded[0]),
            ColumnType.UByte => _view.ReadByte(offset).CompareTo(encoded[0]),
            ColumnType.SByte => ((sbyte)_view.ReadByte(offset)).CompareTo((sbyte)encoded[0]),
            ColumnType.UShort => _view.ReadUInt16(offset).CompareTo(BinaryPrimitives.ReadUInt16LittleEndian(encoded)),
            ColumnType.SShort => _view.ReadInt16(offset).CompareTo(BinaryPrimitives.ReadInt16LittleEndian(encoded)),
            ColumnType.UInt => _view.ReadUInt32(offset).CompareTo(BinaryPrimitives.ReadUInt32LittleEndian(encoded)),
            ColumnType.SInt => _view.ReadInt32(offset).CompareTo(BinaryPrimitives.ReadInt32LittleEndian(encoded)),
            ColumnType.ULong => _view.ReadUInt64(offset).CompareTo(BinaryPrimitives.ReadUInt64LittleEndian(encoded)),
            ColumnType.SLong => _view.ReadInt64(offset).CompareTo(BinaryPrimitives.ReadInt64LittleEndian(encoded)),
            ColumnType.Float => _view.ReadSingle(offset).CompareTo(BinaryPrimitives.ReadSingleLittleEndian(encoded)),
            ColumnType.Double => _view.ReadDouble(offset).CompareTo(BinaryPrimitives.ReadDoubleLittleEndian(encoded)),
            ColumnType.Date => _view.ReadInt32(offset).CompareTo(BinaryPrimitives.ReadInt32LittleEndian(encoded)),
            ColumnType.Time => _view.ReadInt64(offset).CompareTo(BinaryPrimitives.ReadInt64LittleEndian(encoded)),
            ColumnType.DateTime => _view.ReadInt64(offset).CompareTo(BinaryPrimitives.ReadInt64LittleEndian(encoded)),
            ColumnType.String => CompareString(offset, encoded),
            _ => 0,
        };
    }

    /// <summary>
    /// Encodes a raw string value into UTF-8 bytes without zero-padding.
    /// Used as the needle for string match operations (contains/starts/ends).
    /// </summary>
    public byte[] EncodeStringBytes(string raw) => Encoding.UTF8.GetBytes(raw);

    /// <summary>
    /// Returns true if the string value at <paramref name="place"/> matches the
    /// <paramref name="needle"/> bytes according to the given <paramref name="op"/>.
    /// Returns false for null values.
    /// </summary>
    public bool StringMatchAtPlace(long place, byte[] needle, CompareOp op)
    {
        var offset = place * Schema.EntrySize;
        if (offset + Schema.EntrySize > _capacity)
            return false;

        if (_view.ReadByte(offset) != StorageConstants.FLAG_VALUE)
            return false;

        var valueOffset = offset + 1;

        // Determine actual stored string length (up to first zero byte)
        int storedLen = 0;
        for (int i = 0; i < Schema.Size; i++)
        {
            if (_view.ReadByte(valueOffset + i) == 0) break;
            storedLen++;
        }

        return op switch
        {
            CompareOp.Contains => BytesContain(valueOffset, storedLen, needle),
            CompareOp.StartsWith => BytesMatchAt(valueOffset, 0, needle, storedLen),
            CompareOp.EndsWith => storedLen >= needle.Length && BytesMatchAt(valueOffset, storedLen - needle.Length, needle, storedLen),
            _ => false,
        };
    }

    private bool BytesMatchAt(long baseOffset, int startIndex, byte[] needle, int storedLen)
    {
        if (startIndex + needle.Length > storedLen)
            return false;

        for (int i = 0; i < needle.Length; i++)
        {
            if (_view.ReadByte(baseOffset + startIndex + i) != needle[i])
                return false;
        }
        return true;
    }

    private bool BytesContain(long baseOffset, int storedLen, byte[] needle)
    {
        if (needle.Length > storedLen)
            return false;

        var limit = storedLen - needle.Length;
        for (int start = 0; start <= limit; start++)
        {
            if (BytesMatchAt(baseOffset, start, needle, storedLen))
                return true;
        }
        return false;
    }

    private int CompareString(long offset, byte[] encoded)
    {
        // Byte-by-byte comparison (UTF-8 preserves order for ASCII)
        for (int i = 0; i < encoded.Length; i++)
        {
            var stored = _view.ReadByte(offset + i);
            var cmp = stored.CompareTo(encoded[i]);
            if (cmp != 0) return cmp;
        }
        return 0;
    }

    private void EncodeIntoBuffer(byte[] buf, string raw)
    {
        switch (Type)
        {
            case ColumnType.Bool:
                buf[0] = (byte)(raw == "true" ? 1 : 0);
                break;
            case ColumnType.SByte:
                buf[0] = (byte)sbyte.Parse(raw, CultureInfo.InvariantCulture);
                break;
            case ColumnType.UByte:
                buf[0] = byte.Parse(raw, CultureInfo.InvariantCulture);
                break;
            case ColumnType.SShort:
                BinaryPrimitives.WriteInt16LittleEndian(buf, short.Parse(raw, CultureInfo.InvariantCulture));
                break;
            case ColumnType.UShort:
                BinaryPrimitives.WriteUInt16LittleEndian(buf, ushort.Parse(raw, CultureInfo.InvariantCulture));
                break;
            case ColumnType.SInt:
                BinaryPrimitives.WriteInt32LittleEndian(buf, int.Parse(raw, CultureInfo.InvariantCulture));
                break;
            case ColumnType.UInt:
                BinaryPrimitives.WriteUInt32LittleEndian(buf, uint.Parse(raw, CultureInfo.InvariantCulture));
                break;
            case ColumnType.SLong:
                BinaryPrimitives.WriteInt64LittleEndian(buf, long.Parse(raw, CultureInfo.InvariantCulture));
                break;
            case ColumnType.ULong:
                BinaryPrimitives.WriteUInt64LittleEndian(buf, ulong.Parse(raw, CultureInfo.InvariantCulture));
                break;
            case ColumnType.Float:
                BinaryPrimitives.WriteSingleLittleEndian(buf, float.Parse(raw, CultureInfo.InvariantCulture));
                break;
            case ColumnType.Double:
                BinaryPrimitives.WriteDoubleLittleEndian(buf, double.Parse(raw, CultureInfo.InvariantCulture));
                break;
            case ColumnType.Date:
                BinaryPrimitives.WriteInt32LittleEndian(buf,
                    DateOnly.Parse(raw, CultureInfo.InvariantCulture).DayNumber);
                break;
            case ColumnType.Time:
                BinaryPrimitives.WriteInt64LittleEndian(buf,
                    TimeOnly.Parse(raw, CultureInfo.InvariantCulture).Ticks);
                break;
            case ColumnType.DateTime:
                BinaryPrimitives.WriteInt64LittleEndian(buf,
                    DateTime.Parse(raw, CultureInfo.InvariantCulture, DateTimeStyles.AdjustToUniversal).Ticks);
                break;
            case ColumnType.String:
                var bytes = Encoding.UTF8.GetBytes(raw);
                var len = Math.Min(bytes.Length, Schema.Size - 1);
                Array.Copy(bytes, buf, len);
                break;
            case ColumnType.Blob:
                BinaryPrimitives.WriteInt64LittleEndian(buf, long.Parse(raw, CultureInfo.InvariantCulture));
                break;
            case ColumnType.Array:
                BinaryPrimitives.WriteInt64LittleEndian(buf, long.Parse(raw, CultureInfo.InvariantCulture));
                break;
        }
    }

    // ── Capacity ────────────────────────────────────────────

    public void EnsureCapacity(long requiredEntries)
    {
        var requiredBytes = requiredEntries * Schema.EntrySize;
        if (requiredBytes <= _capacity)
            return;

        var newCapacity = _capacity;
        while (newCapacity < requiredBytes)
            newCapacity += (long)_chunkSize * Schema.EntrySize;

        Remap(newCapacity);
    }

    // ── Encode (write typed value at offset) ────────────────

    private object EncodeAndWrite(long offset, string raw)
    {
        return Type switch
        {
            ColumnType.Bool => WriteBool(offset, raw),
            ColumnType.SByte => WriteSByte(offset, raw),
            ColumnType.UByte => WriteUByte(offset, raw),
            ColumnType.SShort => WriteSShort(offset, raw),
            ColumnType.UShort => WriteUShort(offset, raw),
            ColumnType.SInt => WriteSInt(offset, raw),
            ColumnType.UInt => WriteUInt(offset, raw),
            ColumnType.SLong => WriteSLong(offset, raw),
            ColumnType.ULong => WriteULong(offset, raw),
            ColumnType.Float => WriteFloat(offset, raw),
            ColumnType.Double => WriteDouble(offset, raw),
            ColumnType.Date => WriteDate(offset, raw),
            ColumnType.Time => WriteTime(offset, raw),
            ColumnType.DateTime => WriteDateTime(offset, raw),
            ColumnType.String => WriteString(offset, raw),
            ColumnType.Blob => WriteBlob(offset, raw),
            ColumnType.Array => WriteArray(offset, raw),
            _ => throw new ArgumentOutOfRangeException(),
        };
    }

    private object WriteBool(long offset, string raw)
    {
        var val = raw == "true";
        _view.Write(offset, val ? (byte)1 : (byte)0);
        return val;
    }

    private object WriteSByte(long offset, string raw)
    {
        var val = sbyte.Parse(raw, CultureInfo.InvariantCulture);
        _view.Write(offset, (byte)val);
        return val;
    }

    private object WriteUByte(long offset, string raw)
    {
        var val = byte.Parse(raw, CultureInfo.InvariantCulture);
        _view.Write(offset, val);
        return val;
    }

    private object WriteSShort(long offset, string raw)
    {
        var val = short.Parse(raw, CultureInfo.InvariantCulture);
        _view.Write(offset, val);
        return val;
    }

    private object WriteUShort(long offset, string raw)
    {
        var val = ushort.Parse(raw, CultureInfo.InvariantCulture);
        _view.Write(offset, val);
        return val;
    }

    private object WriteSInt(long offset, string raw)
    {
        var val = int.Parse(raw, CultureInfo.InvariantCulture);
        _view.Write(offset, val);
        return val;
    }

    private object WriteUInt(long offset, string raw)
    {
        var val = uint.Parse(raw, CultureInfo.InvariantCulture);
        _view.Write(offset, val);
        return val;
    }

    private object WriteSLong(long offset, string raw)
    {
        var val = long.Parse(raw, CultureInfo.InvariantCulture);
        _view.Write(offset, val);
        return val;
    }

    private object WriteULong(long offset, string raw)
    {
        var val = ulong.Parse(raw, CultureInfo.InvariantCulture);
        _view.Write(offset, val);
        return val;
    }

    private object WriteFloat(long offset, string raw)
    {
        var val = float.Parse(raw, CultureInfo.InvariantCulture);
        _view.Write(offset, val);
        return val;
    }

    private object WriteDouble(long offset, string raw)
    {
        var val = double.Parse(raw, CultureInfo.InvariantCulture);
        _view.Write(offset, val);
        return val;
    }

    private object WriteDate(long offset, string raw)
    {
        var val = DateOnly.Parse(raw, CultureInfo.InvariantCulture);
        _view.Write(offset, val.DayNumber);
        return val.ToString("yyyy-MM-dd");
    }

    private object WriteTime(long offset, string raw)
    {
        var val = TimeOnly.Parse(raw, CultureInfo.InvariantCulture);
        _view.Write(offset, val.Ticks);
        return val.ToString("HH:mm:ss");
    }

    private object WriteDateTime(long offset, string raw)
    {
        var val = DateTime.Parse(raw, CultureInfo.InvariantCulture, DateTimeStyles.AdjustToUniversal);
        _view.Write(offset, val.Ticks);
        return val.ToString("yyyy-MM-dd HH:mm:ss");
    }

    private object WriteString(long offset, string raw)
    {
        var bytes = Encoding.UTF8.GetBytes(raw);
        var len = Math.Min(bytes.Length, Schema.Size - 1); // room for null terminator
        // Zero the entire value area first
        var zeroBuf = new byte[Schema.Size];
        _view.WriteArray(offset, zeroBuf, 0, zeroBuf.Length);
        // Write string bytes
        _view.WriteArray(offset, bytes, 0, len);
        return raw;
    }

    /// <summary>
    /// Writes the blob byte count into the .col file.
    /// The raw value is the byte count as a string (set by the executor after writing the .blob file).
    /// </summary>
    private object WriteBlob(long offset, string raw)
    {
        var byteCount = long.Parse(raw, CultureInfo.InvariantCulture);
        _view.Write(offset, byteCount);
        return byteCount;
    }

    /// <summary>
    /// Writes the array element count into the .col file.
    /// The raw value is the element count as a string (set by the executor after writing the .array file).
    /// </summary>
    private object WriteArray(long offset, string raw)
    {
        var elementCount = long.Parse(raw, CultureInfo.InvariantCulture);
        _view.Write(offset, elementCount);
        return elementCount;
    }

    // ── Decode (read typed value at offset) ─────────────────

    private object ReadAndDecode(long offset)
    {
        return Type switch
        {
            ColumnType.Bool => _view.ReadByte(offset) != 0,
            ColumnType.SByte => (sbyte)_view.ReadByte(offset),
            ColumnType.UByte => _view.ReadByte(offset),
            ColumnType.SShort => _view.ReadInt16(offset),
            ColumnType.UShort => _view.ReadUInt16(offset),
            ColumnType.SInt => _view.ReadInt32(offset),
            ColumnType.UInt => _view.ReadUInt32(offset),
            ColumnType.SLong => _view.ReadInt64(offset),
            ColumnType.ULong => _view.ReadUInt64(offset),
            ColumnType.Float => _view.ReadSingle(offset),
            ColumnType.Double => _view.ReadDouble(offset),
            ColumnType.Date => ReadDate(offset),
            ColumnType.Time => ReadTime(offset),
            ColumnType.DateTime => ReadDateTime(offset),
            ColumnType.String => ReadString(offset),
            ColumnType.Blob => _view.ReadInt64(offset),
            ColumnType.Array => _view.ReadInt64(offset),
            _ => throw new ArgumentOutOfRangeException(),
        };
    }

    private string ReadDate(long offset)
    {
        var dayNumber = _view.ReadInt32(offset);
        return DateOnly.FromDayNumber(dayNumber).ToString("yyyy-MM-dd");
    }

    private string ReadTime(long offset)
    {
        var ticks = _view.ReadInt64(offset);
        return new TimeOnly(ticks).ToString("HH:mm:ss");
    }

    private string ReadDateTime(long offset)
    {
        var ticks = _view.ReadInt64(offset);
        return new DateTime(ticks, DateTimeKind.Utc).ToString("yyyy-MM-dd HH:mm:ss");
    }

    private string ReadString(long offset)
    {
        var buf = new byte[Schema.Size];
        _view.ReadArray(offset, buf, 0, buf.Length);
        var end = Array.IndexOf(buf, (byte)0);
        if (end < 0) end = buf.Length;
        return Encoding.UTF8.GetString(buf, 0, end);
    }

    // ── Flush ────────────────────────────────────────────────

    public void Flush()
    {
        _view.Flush();
    }

    // ── MMF lifecycle ───────────────────────────────────────

    private void Remap(long newCapacity)
    {
        var oldView = _view;
        var oldMmf = _mmf;

        oldView.Flush();

        _fs.SetLength(newCapacity);
        _capacity = newCapacity;
        (_mmf, _view) = CreateMapping(_fs, _capacity);

        // Dispose old mappings after new ones are live
        oldView.Dispose();
        oldMmf.Dispose();
    }

    private static (MemoryMappedFile, MemoryMappedViewAccessor) CreateMapping(FileStream fs, long capacity)
    {
        var mmf = MemoryMappedFile.CreateFromFile(
            fs, mapName: null, capacity,
            MemoryMappedFileAccess.ReadWrite,
            HandleInheritability.None, leaveOpen: true);
        var view = mmf.CreateViewAccessor(0, capacity, MemoryMappedFileAccess.ReadWrite);
        return (mmf, view);
    }

    public void Dispose()
    {
        _view.Dispose();
        _mmf.Dispose();
        _fs.Dispose();
    }
}

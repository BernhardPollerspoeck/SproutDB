using System.Buffers.Binary;
using System.Globalization;
using System.IO.MemoryMappedFiles;
using System.Text;

namespace SproutDB.Core.Storage;

internal sealed class ColumnHandle : IDisposable
{
    private readonly string _path;
    private FileStream _fs;
    private MemoryMappedFile _mmf;
    private MemoryMappedViewAccessor _view;
    private long _capacity;

    public ColumnSchemaEntry Schema { get; }
    public ColumnType Type { get; }

    public ColumnHandle(string path, ColumnSchemaEntry schema)
    {
        _path = path;
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

    // ── Capacity ────────────────────────────────────────────

    public void EnsureCapacity(long requiredEntries)
    {
        var requiredBytes = requiredEntries * Schema.EntrySize;
        if (requiredBytes <= _capacity)
            return;

        var newCapacity = _capacity;
        while (newCapacity < requiredBytes)
            newCapacity += (long)StorageConstants.CHUNK_SIZE * Schema.EntrySize;

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
        _view.Flush();
        _view.Dispose();
        _mmf.Dispose();

        _fs.SetLength(newCapacity);
        _capacity = newCapacity;
        (_mmf, _view) = CreateMapping(_fs, _capacity);
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

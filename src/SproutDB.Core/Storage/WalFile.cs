using System.Buffers.Binary;
using System.Text;

namespace SproutDB.Core.Storage;

internal sealed class WalFile : IDisposable
{
    private const int HeaderSize = 28; // int64 + uint64 + int32 + int64
    private readonly FileStream _fs;
    private long _nextSequence;
    private bool _dirty;

    public WalFile(string path)
    {
        _fs = new FileStream(path, FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.Read);
        _nextSequence = ScanLastSequence() + 1;
    }

    /// <summary>
    /// Appends a WAL entry to the OS buffer (no fsync).
    /// Call <see cref="SyncToDisk"/> to flush to durable storage.
    /// Format: [Sequence: int64][ResolvedId: uint64][QueryLength: int32][GroupId: int64][Query: UTF-8]
    /// </summary>
    public long Append(string query, ulong resolvedId = 0, long groupId = 0)
    {
        var seq = _nextSequence++;
        var queryBytes = Encoding.UTF8.GetBytes(query);

        Span<byte> header = stackalloc byte[HeaderSize];
        BinaryPrimitives.WriteInt64LittleEndian(header, seq);
        BinaryPrimitives.WriteUInt64LittleEndian(header[8..], resolvedId);
        BinaryPrimitives.WriteInt32LittleEndian(header[16..], queryBytes.Length);
        BinaryPrimitives.WriteInt64LittleEndian(header[20..], groupId);

        _fs.Seek(0, SeekOrigin.End);
        _fs.Write(header);
        _fs.Write(queryBytes);
        _fs.Flush(flushToDisk: false);
        _dirty = true;

        return seq;
    }

    /// <summary>
    /// Flushes pending WAL data to durable storage (fsync).
    /// Called periodically by the engine's WAL sync cycle.
    /// </summary>
    public void SyncToDisk()
    {
        if (!_dirty) return;
        _fs.Flush(flushToDisk: true);
        _dirty = false;
    }

    /// <summary>
    /// Reads all WAL entries from the beginning.
    /// </summary>
    public List<WalEntry> ReadAll()
    {
        var entries = new List<WalEntry>();
        _fs.Seek(0, SeekOrigin.Begin);
        var headerBuf = new byte[HeaderSize];

        while (_fs.Position < _fs.Length)
        {
            if (_fs.Read(headerBuf, 0, HeaderSize) != HeaderSize) break;

            var seq = BinaryPrimitives.ReadInt64LittleEndian(headerBuf);
            var resolvedId = BinaryPrimitives.ReadUInt64LittleEndian(headerBuf.AsSpan(8));
            var len = BinaryPrimitives.ReadInt32LittleEndian(headerBuf.AsSpan(16));
            var groupId = BinaryPrimitives.ReadInt64LittleEndian(headerBuf.AsSpan(20));

            var queryBuf = new byte[len];
            if (_fs.Read(queryBuf, 0, len) != len) break;

            entries.Add(new WalEntry
            {
                Sequence = seq,
                ResolvedId = resolvedId,
                Query = Encoding.UTF8.GetString(queryBuf),
                GroupId = groupId,
            });
        }

        return entries;
    }

    /// <summary>
    /// Marks all entries with the given groupId as rolled back by overwriting
    /// the groupId to its negative value.
    /// </summary>
    public void MarkGroupRolledBack(long groupId)
    {
        if (groupId <= 0) return;

        _fs.Seek(0, SeekOrigin.Begin);
        var headerBuf = new byte[HeaderSize];

        while (_fs.Position < _fs.Length)
        {
            var entryStart = _fs.Position;
            if (_fs.Read(headerBuf, 0, HeaderSize) != HeaderSize) break;

            var len = BinaryPrimitives.ReadInt32LittleEndian(headerBuf.AsSpan(16));
            var entryGroupId = BinaryPrimitives.ReadInt64LittleEndian(headerBuf.AsSpan(20));

            if (entryGroupId == groupId)
            {
                // Overwrite groupId to negative (rolled back marker)
                var negGroupBuf = new byte[8];
                BinaryPrimitives.WriteInt64LittleEndian(negGroupBuf, -groupId);
                _fs.Seek(entryStart + 20, SeekOrigin.Begin);
                _fs.Write(negGroupBuf);
                _fs.Seek(entryStart + HeaderSize + len, SeekOrigin.Begin);
            }
            else
            {
                _fs.Seek(len, SeekOrigin.Current);
            }
        }

        _fs.Flush(flushToDisk: false);
        _dirty = true;
    }

    /// <summary>
    /// Truncates the WAL file and resets sequence counter.
    /// </summary>
    public void Truncate()
    {
        _fs.SetLength(0);
        _fs.Flush(flushToDisk: true);
        _nextSequence = 1;
    }

    public bool IsEmpty => _fs.Length == 0;

    public long SizeBytes => _fs.Length;

    /// <summary>
    /// Returns the next groupId for transaction grouping.
    /// </summary>
    public long NextGroupId() => _nextSequence;

    private long ScanLastSequence()
    {
        long last = 0;
        _fs.Seek(0, SeekOrigin.Begin);
        var headerBuf = new byte[HeaderSize];

        while (_fs.Position < _fs.Length)
        {
            if (_fs.Read(headerBuf, 0, HeaderSize) != HeaderSize) break;

            last = BinaryPrimitives.ReadInt64LittleEndian(headerBuf);
            var len = BinaryPrimitives.ReadInt32LittleEndian(headerBuf.AsSpan(16));

            _fs.Seek(len, SeekOrigin.Current);
        }

        return last;
    }

    public void Dispose()
    {
        _fs.Dispose();
    }
}

using System.Buffers.Binary;
using System.Text;

namespace SproutDB.Core.Storage;

internal sealed class WalFile : IDisposable
{
    private readonly FileStream _fs;
    private long _nextSequence;

    public WalFile(string path)
    {
        _fs = new FileStream(path, FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.Read);
        _nextSequence = ScanLastSequence() + 1;
    }

    /// <summary>
    /// Appends a WAL entry and fsyncs to disk.
    /// Format: [Sequence: int64][QueryLength: int32][Query: UTF-8]
    /// </summary>
    public long Append(string query)
    {
        var seq = _nextSequence++;
        var queryBytes = Encoding.UTF8.GetBytes(query);

        Span<byte> header = stackalloc byte[12];
        BinaryPrimitives.WriteInt64LittleEndian(header, seq);
        BinaryPrimitives.WriteInt32LittleEndian(header[8..], queryBytes.Length);

        _fs.Seek(0, SeekOrigin.End);
        _fs.Write(header);
        _fs.Write(queryBytes);
        _fs.Flush(flushToDisk: true);

        return seq;
    }

    /// <summary>
    /// Reads all WAL entries from the beginning.
    /// </summary>
    public List<WalEntry> ReadAll()
    {
        var entries = new List<WalEntry>();
        _fs.Seek(0, SeekOrigin.Begin);
        var headerBuf = new byte[12];

        while (_fs.Position < _fs.Length)
        {
            if (_fs.Read(headerBuf, 0, 12) != 12) break;

            var seq = BinaryPrimitives.ReadInt64LittleEndian(headerBuf);
            var len = BinaryPrimitives.ReadInt32LittleEndian(headerBuf.AsSpan(8));

            var queryBuf = new byte[len];
            if (_fs.Read(queryBuf, 0, len) != len) break;

            entries.Add(new WalEntry
            {
                Sequence = seq,
                Query = Encoding.UTF8.GetString(queryBuf),
            });
        }

        return entries;
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

    private long ScanLastSequence()
    {
        long last = 0;
        _fs.Seek(0, SeekOrigin.Begin);
        var headerBuf = new byte[12];

        while (_fs.Position < _fs.Length)
        {
            if (_fs.Read(headerBuf, 0, 12) != 12) break;

            last = BinaryPrimitives.ReadInt64LittleEndian(headerBuf);
            var len = BinaryPrimitives.ReadInt32LittleEndian(headerBuf.AsSpan(8));

            _fs.Seek(len, SeekOrigin.Current);
        }

        return last;
    }

    public void Dispose()
    {
        _fs.Dispose();
    }
}

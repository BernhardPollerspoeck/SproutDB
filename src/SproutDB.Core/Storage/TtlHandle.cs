using System.IO.MemoryMappedFiles;

namespace SproutDB.Core.Storage;

/// <summary>
/// MMF-based TTL storage file (_ttl).
///
/// Layout per row (16 bytes):
///   [0..7]   ExpiresAt      (long) – absolute UTC ms since epoch, 0 = no expiry
///   [8..15]  RowTtlDuration (long) – configured duration in seconds, 0 = no row TTL
///
/// Row at place P starts at offset P × 16.
/// </summary>
internal sealed class TtlHandle : IDisposable
{
    private const int ENTRY_SIZE = 16;

    private readonly string _path;
    private readonly int _chunkSize;
    private FileStream _fs;
    private MemoryMappedFile _mmf;
    private MemoryMappedViewAccessor _view;
    private long _fileCapacity;

    public TtlHandle(string path, int chunkSize = StorageConstants.CHUNK_SIZE)
    {
        _path = path;
        _chunkSize = chunkSize;
        _fs = new FileStream(path, FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.ReadWrite);

        if (_fs.Length == 0)
            _fs.SetLength((long)chunkSize * ENTRY_SIZE);

        _fileCapacity = _fs.Length;
        _mmf = MemoryMappedFile.CreateFromFile(_fs, null, _fileCapacity, MemoryMappedFileAccess.ReadWrite, HandleInheritability.None, leaveOpen: true);
        _view = _mmf.CreateViewAccessor(0, _fileCapacity, MemoryMappedFileAccess.ReadWrite);
    }

    public static void CreateNew(string path, int chunkSize = StorageConstants.CHUNK_SIZE)
    {
        using var fs = File.Create(path);
        fs.SetLength((long)chunkSize * ENTRY_SIZE);
    }

    public long ReadExpiresAt(long place)
    {
        var offset = place * ENTRY_SIZE;
        if (offset + ENTRY_SIZE > _fileCapacity) return 0;
        return _view.ReadInt64(offset);
    }

    public long ReadRowTtlDuration(long place)
    {
        var offset = place * ENTRY_SIZE + 8;
        if (offset + 8 > _fileCapacity) return 0;
        return _view.ReadInt64(offset);
    }

    public void Write(long place, long expiresAtMs, long rowTtlSeconds)
    {
        EnsureCapacity(place + 1);
        var offset = place * ENTRY_SIZE;
        _view.Write(offset, expiresAtMs);
        _view.Write(offset + 8, rowTtlSeconds);
    }

    public void Clear(long place)
    {
        var offset = place * ENTRY_SIZE;
        if (offset + ENTRY_SIZE > _fileCapacity) return;
        _view.Write(offset, 0L);
        _view.Write(offset + 8, 0L);
    }

    public void EnsureCapacity(long rowCount)
    {
        var needed = rowCount * ENTRY_SIZE;
        if (needed <= _fileCapacity) return;

        // Grow by chunks
        var newCapacity = _fileCapacity;
        var chunkBytes = (long)_chunkSize * ENTRY_SIZE;
        while (newCapacity < needed)
            newCapacity += chunkBytes;

        _view.Dispose();
        _mmf.Dispose();

        _fs.SetLength(newCapacity);
        _fileCapacity = newCapacity;

        _mmf = MemoryMappedFile.CreateFromFile(_fs, null, _fileCapacity, MemoryMappedFileAccess.ReadWrite, HandleInheritability.None, leaveOpen: true);
        _view = _mmf.CreateViewAccessor(0, _fileCapacity, MemoryMappedFileAccess.ReadWrite);
    }

    public void Flush()
    {
        _view.Flush();
    }

    public void Dispose()
    {
        _view.Dispose();
        _mmf.Dispose();
        _fs.Dispose();
    }
}

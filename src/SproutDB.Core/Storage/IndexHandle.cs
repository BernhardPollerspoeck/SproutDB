using System.IO.MemoryMappedFiles;

namespace SproutDB.Core.Storage;

/// <summary>
/// Slot-based index file.
///
/// Layout:
///   [Header: 20 bytes][Slot 0: 8B][Slot 1: 8B][Slot 2: 8B]...
///
/// Header:
///   [0..3]   Count      (int)  – active rows
///   [4..11]  NextId     (long) – next ID to assign (monotonic, never recycled)
///   [12..15] LowestUsed (int)  – hint: lowest slot index that might be occupied
///   [16..19] TotalSlots (int)  – total slot capacity
///
/// Each slot stores an ID (long). 0 = empty/free.
/// Place = slot index. Column files use Place × EntrySize.
/// </summary>
internal sealed class IndexHandle : IDisposable
{
    private const int HEADER_SIZE = 20;
    private const int SLOT_SIZE = sizeof(long); // 8 bytes

    private readonly string _path;
    private readonly int _chunkSize;
    private FileStream _fs;
    private MemoryMappedFile _mmf;
    private MemoryMappedViewAccessor _view;
    private long _fileCapacity;

    // In-memory header cache
    private int _count;
    private long _nextId;
    private int _lowestUsed;
    private int _totalSlots;
    private int _nextFreeSlot; // sequential allocation cursor

    public IndexHandle(string path, int chunkSize = StorageConstants.CHUNK_SIZE)
    {
        _path = path;
        _chunkSize = chunkSize;
        _fs = new FileStream(path, FileMode.Open, FileAccess.ReadWrite, FileShare.ReadWrite);
        _fileCapacity = _fs.Length;
        (_mmf, _view) = CreateMapping(_fs, _fileCapacity);
        ReadHeader();
        RebuildState();
    }

    public long ActiveRowCount => _count;

    // ── NextId ───────────────────────────────────────────────

    public ulong ReadNextId() => (ulong)_nextId;

    public void WriteNextId(ulong nextId, TransactionJournal? journal = null)
    {
        journal?.RecordIndexWriteNextId(this);
        _nextId = (long)nextId;
        _view.Write(4, _nextId);
    }

    // ── Slot operations ──────────────────────────────────────

    /// <summary>
    /// Finds the place (slot index) for a given ID by scanning slots.
    /// Returns -1 if not found. O(count) — used for explicit _id upsert/delete.
    /// </summary>
    public long ReadPlace(ulong id)
    {
        var targetId = (long)id;
        int targetCount = _count;
        int found = 0;
        for (int slot = 0; slot < _totalSlots && found < targetCount; slot++)
        {
            var slotId = ReadSlotId(slot);
            if (slotId <= 0) continue;
            if (slotId == targetId) return slot;
            found++;
        }
        return -1;
    }

    /// <summary>
    /// Writes an ID into a specific slot. Increments count.
    /// </summary>
    public void WritePlace(ulong id, long place, TransactionJournal? journal = null)
    {
        journal?.RecordIndexWritePlace(this, place);
        var slot = (int)place;
        WriteSlotId(slot, (long)id);
        _count++;
        WriteCount();
        if (slot < _lowestUsed || _count == 1)
        {
            _lowestUsed = slot;
            WriteLowestUsed();
        }
    }

    /// <summary>
    /// Marks a slot as free by scanning for the ID. Decrements count.
    /// Prefer FreeSlot(place) when you already know the place.
    /// </summary>
    public void ClearPlace(ulong id)
    {
        var place = ReadPlace(id);
        if (place < 0) return;
        FreeSlot(place);
    }

    /// <summary>
    /// Frees a slot directly (write 0, decrement count).
    /// Use when you already have the place from iteration.
    /// </summary>
    public void FreeSlot(long place, TransactionJournal? journal = null)
    {
        journal?.RecordIndexFreeSlot(this, place);
        WriteSlotId((int)place, 0);
        _count--;
        WriteCount();
    }

    /// <summary>
    /// No-op. Place freeing is fully handled by FreeSlot/ClearPlace.
    /// Kept for backward compatibility during migration.
    /// </summary>
    public void AddFreePlace(long place) { }

    /// <summary>
    /// Finds the next free slot for a new row.
    /// Sequential fill → backfill when ≥20% free at block end → grow.
    /// </summary>
    public long FindNextPlace()
    {
        // Try sequential allocation from current cursor
        for (int slot = _nextFreeSlot; slot < _totalSlots; slot++)
        {
            if (ReadSlotId(slot) == 0)
            {
                _nextFreeSlot = slot + 1;
                return slot;
            }
        }

        // Reached end — check backfill threshold
        int freeCount = _totalSlots - _count;
        if (freeCount > 0 && _totalSlots > 0)
        {
            double freeRatio = (double)freeCount / _totalSlots;
            if (freeRatio >= StorageConstants.BACKFILL_THRESHOLD)
            {
                // Backfill: scan from beginning for first free slot
                for (int slot = 0; slot < _totalSlots; slot++)
                {
                    if (ReadSlotId(slot) == 0)
                    {
                        _nextFreeSlot = slot + 1;
                        return slot;
                    }
                }
            }
        }

        // No free slots or below threshold — grow
        GrowSlots();
        var newSlot = _nextFreeSlot;
        _nextFreeSlot = newSlot + 1;
        return newSlot;
    }

    /// <summary>
    /// O(1) reverse lookup: reads the ID stored at a given slot.
    /// Returns 0 if the slot is empty.
    /// </summary>
    public ulong FindIdForPlace(long place)
    {
        if (place < 0 || (int)place >= _totalSlots) return 0;
        var id = ReadSlotId((int)place);
        return id > 0 ? (ulong)id : 0;
    }

    /// <summary>
    /// Iterates all occupied slots. Calls action(id, place) for each active row.
    /// Uses snapshot of count for early termination.
    /// Safe for concurrent reads, but do not add new slots during iteration.
    /// </summary>
    public void ForEachUsed(Action<ulong, long> action)
    {
        int targetCount = _count;
        int found = 0;
        for (int slot = 0; slot < _totalSlots && found < targetCount; slot++)
        {
            var id = ReadSlotId(slot);
            if (id <= 0) continue;
            found++;
            action((ulong)id, slot);
        }
    }

    // ── Transaction rollback helpers ─────────────────────────

    /// <summary>
    /// Restores count and nextId to previous values (used by TransactionJournal).
    /// </summary>
    internal void RestoreState(long count, ulong nextId)
    {
        _count = (int)count;
        WriteCount();
        _nextId = (long)nextId;
        _view.Write(4, _nextId);
    }

    /// <summary>
    /// Restores count to a previous value (used by TransactionJournal).
    /// </summary>
    internal void RestoreCount(long count)
    {
        _count = (int)count;
        WriteCount();
    }

    // ── File management ──────────────────────────────────────

    public void Flush()
    {
        FlushHeader();
        _view.Flush();
    }

    public void EnsureCapacity(long requiredBytes)
    {
        if (requiredBytes <= _fileCapacity)
            return;

        var newCapacity = _fileCapacity;
        while (newCapacity < requiredBytes)
            newCapacity += (long)_chunkSize * SLOT_SIZE;

        Remap(newCapacity);
    }

    public void Dispose()
    {
        FlushHeader();
        _view.Dispose();
        _mmf.Dispose();
        _fs.Dispose();
    }

    // ── Static creation ──────────────────────────────────────

    /// <summary>
    /// Creates a new index file with the slot-based format.
    /// </summary>
    public static void CreateNew(string path, int chunkSize = StorageConstants.CHUNK_SIZE)
    {
        var totalSlots = chunkSize;
        var fileSize = HEADER_SIZE + (long)totalSlots * SLOT_SIZE;

        using var fs = File.Create(path);
        fs.SetLength(fileSize);

        // Write header
        Span<byte> header = stackalloc byte[HEADER_SIZE];
        BitConverter.TryWriteBytes(header[0..4], 0);            // Count = 0
        BitConverter.TryWriteBytes(header[4..12], 1L);          // NextId = 1
        BitConverter.TryWriteBytes(header[12..16], 0);          // LowestUsed = 0
        BitConverter.TryWriteBytes(header[16..20], totalSlots); // TotalSlots
        fs.Seek(0, SeekOrigin.Begin);
        fs.Write(header);
    }

    // ── Private helpers ──────────────────────────────────────

    private long ReadSlotId(int slot)
    {
        var offset = HEADER_SIZE + (long)slot * SLOT_SIZE;
        if (offset + SLOT_SIZE > _fileCapacity) return 0;
        return _view.ReadInt64(offset);
    }

    private void WriteSlotId(int slot, long id)
    {
        var offset = HEADER_SIZE + (long)slot * SLOT_SIZE;
        var requiredCapacity = offset + SLOT_SIZE;
        if (requiredCapacity > _fileCapacity)
            Remap(requiredCapacity + (long)_chunkSize * SLOT_SIZE);
        _view.Write(offset, id);
    }

    private void ReadHeader()
    {
        if (_fileCapacity < HEADER_SIZE)
        {
            _count = 0;
            _nextId = 1;
            _lowestUsed = 0;
            _totalSlots = 0;
            return;
        }

        _count = _view.ReadInt32(0);
        _nextId = _view.ReadInt64(4);
        _lowestUsed = _view.ReadInt32(12);
        _totalSlots = _view.ReadInt32(16);
    }

    private void FlushHeader()
    {
        if (_fileCapacity < HEADER_SIZE) return;
        _view.Write(0, _count);
        _view.Write(4, _nextId);
        _view.Write(12, _lowestUsed);
        _view.Write(16, _totalSlots);
    }

    private void WriteCount()
    {
        _view.Write(0, _count);
    }

    private void WriteLowestUsed()
    {
        _view.Write(12, _lowestUsed);
    }

    /// <summary>
    /// Scans slots at open time to rebuild _nextFreeSlot and validate _count.
    /// </summary>
    private void RebuildState()
    {
        _nextFreeSlot = 0;
        int actualCount = 0;

        for (int slot = 0; slot < _totalSlots; slot++)
        {
            if (ReadSlotId(slot) > 0)
            {
                actualCount++;
                _nextFreeSlot = slot + 1;
            }
        }

        // Fix count if corrupted (e.g. crash before header flush)
        if (_count != actualCount)
        {
            _count = actualCount;
            WriteCount();
        }
    }

    private void GrowSlots()
    {
        var oldTotal = _totalSlots;
        _totalSlots += _chunkSize;
        _view.Write(16, _totalSlots);

        var newFileCapacity = HEADER_SIZE + (long)_totalSlots * SLOT_SIZE;
        if (newFileCapacity > _fileCapacity)
            Remap(newFileCapacity);

        _nextFreeSlot = oldTotal;
    }

    private void Remap(long newCapacity)
    {
        _view.Flush();
        _view.Dispose();
        _mmf.Dispose();

        _fs.SetLength(newCapacity);
        _fileCapacity = newCapacity;
        (_mmf, _view) = CreateMapping(_fs, _fileCapacity);
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
}

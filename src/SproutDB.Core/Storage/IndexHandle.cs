using System.IO.MemoryMappedFiles;

namespace SproutDB.Core.Storage;

internal sealed class IndexHandle : IDisposable
{
    private readonly string _path;
    private readonly int _chunkSize;
    private readonly Queue<long> _freePlaces = new();
    private FileStream _fs;
    private MemoryMappedFile _mmf;
    private MemoryMappedViewAccessor _view;
    private long _capacity;
    private long _nextPlace;

    public IndexHandle(string path, int chunkSize = StorageConstants.CHUNK_SIZE)
    {
        _path = path;
        _chunkSize = chunkSize;
        _fs = new FileStream(path, FileMode.Open, FileAccess.ReadWrite, FileShare.ReadWrite);
        _capacity = _fs.Length;
        (_mmf, _view) = CreateMapping(_fs, _capacity);
        _nextPlace = ScanMaxPlace() + 1;
    }

    // ── NextId (stored at position 0 in the index file) ────

    /// <summary>
    /// Reads next_id from the index header (offset 0).
    /// </summary>
    public ulong ReadNextId()
    {
        return _view.ReadUInt64(0);
    }

    /// <summary>
    /// Writes next_id into the index header (offset 0).
    /// </summary>
    public void WriteNextId(ulong nextId)
    {
        _view.Write(0, nextId);
    }

    // ── ID → Place mapping ───────────────────────────────────

    /// <summary>
    /// Reads the stored place for a given ID.
    /// Returns the place (0-based), or -1 if the ID is free/deleted.
    /// </summary>
    public long ReadPlace(ulong id)
    {
        var offset = (long)id * StorageConstants.INDEX_ENTRY_SIZE;
        if (offset + StorageConstants.INDEX_ENTRY_SIZE > _capacity)
            return -1;

        var storedValue = _view.ReadInt64(offset);
        return storedValue == 0 ? -1 : storedValue - 1;
    }

    /// <summary>
    /// Writes the ID → Place mapping. Stores place+1 (0 = free).
    /// </summary>
    public void WritePlace(ulong id, long place)
    {
        var offset = (long)id * StorageConstants.INDEX_ENTRY_SIZE;
        EnsureCapacity(offset + StorageConstants.INDEX_ENTRY_SIZE);
        _view.Write(offset, place + 1);
    }

    /// <summary>
    /// Marks the given ID as deleted by writing 0 into the index slot.
    /// </summary>
    public void ClearPlace(ulong id)
    {
        var offset = (long)id * StorageConstants.INDEX_ENTRY_SIZE;
        if (offset + StorageConstants.INDEX_ENTRY_SIZE <= _capacity)
            _view.Write(offset, (long)0);
    }

    /// <summary>
    /// Registers a freed place for reuse by future inserts.
    /// </summary>
    public void AddFreePlace(long place) => _freePlaces.Enqueue(place);

    /// <summary>
    /// Returns the next available place — reuses freed places first, then allocates new.
    /// </summary>
    public long FindNextPlace()
    {
        return _freePlaces.Count > 0 ? _freePlaces.Dequeue() : _nextPlace++;
    }

    /// <summary>
    /// Scans all index entries to find the highest used place.
    /// Also rebuilds the free-place queue by identifying gaps.
    /// Called once at open time to initialize _nextPlace.
    /// </summary>
    private long ScanMaxPlace()
    {
        long maxPlace = -1;
        var entries = _capacity / StorageConstants.INDEX_ENTRY_SIZE;
        var usedPlaces = new HashSet<long>();

        for (long i = 1; i < entries; i++)
        {
            var storedValue = _view.ReadInt64(i * StorageConstants.INDEX_ENTRY_SIZE);
            if (storedValue > 0)
            {
                var place = storedValue - 1;
                usedPlaces.Add(place);
                if (place > maxPlace)
                    maxPlace = place;
            }
        }

        // Rebuild free-place queue: any place in [0..maxPlace) not used is free
        for (long p = 0; p < maxPlace; p++)
        {
            if (!usedPlaces.Contains(p))
                _freePlaces.Enqueue(p);
        }

        return maxPlace;
    }

    /// <summary>
    /// Reverse lookup: finds the ID that maps to a given place.
    /// Returns 0 if no ID maps to this place (deleted/invalid).
    /// </summary>
    public ulong FindIdForPlace(long place)
    {
        var entries = _capacity / StorageConstants.INDEX_ENTRY_SIZE;
        var target = place + 1; // stored values are place+1

        for (long i = 1; i < entries; i++)
        {
            var storedValue = _view.ReadInt64(i * StorageConstants.INDEX_ENTRY_SIZE);
            if (storedValue == target)
                return (ulong)i;
        }

        return 0;
    }

    /// <summary>
    /// Iterates all used ID → Place mappings. Calls action(id, place) for each.
    /// </summary>
    public void ForEachUsed(Action<ulong, long> action)
    {
        var entries = _capacity / StorageConstants.INDEX_ENTRY_SIZE;

        for (long i = 1; i < entries; i++)
        {
            var storedValue = _view.ReadInt64(i * StorageConstants.INDEX_ENTRY_SIZE);
            if (storedValue > 0)
                action((ulong)i, storedValue - 1);
        }
    }

    public void Flush()
    {
        _view.Flush();
    }

    public void EnsureCapacity(long requiredBytes)
    {
        if (requiredBytes <= _capacity)
            return;

        var newCapacity = _capacity;
        while (newCapacity < requiredBytes)
            newCapacity += (long)(_chunkSize + 1) * StorageConstants.INDEX_ENTRY_SIZE;

        Remap(newCapacity);
    }

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

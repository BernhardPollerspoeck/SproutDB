using System.Buffers.Binary;
using System.IO.MemoryMappedFiles;
using System.Text;

namespace SproutDB.Core.Storage;

/// <summary>
/// Disk-based B-Tree index mapping column values to storage places.
/// Memory-mapped file following the same pattern as ColumnHandle and IndexHandle.
///
/// File layout:
///   Header (32 bytes):
///     [0..3]   magic "SBTI"
///     [4]      version (1)
///     [5]      reserved
///     [6..7]   order (max keys per node)
///     [8..15]  root_offset (0 = empty tree)
///     [16..23] node_count
///     [24..31] free_node_offset (0 = none)
///
///   Node layout (size = NodeSize):
///     [0]     flags (bit 0: is_leaf)
///     [1..2]  key_count
///     [3..]   key_count × (key_size + 8 bytes place)
///     then for internal nodes: (key_count + 1) × 8 bytes child offsets
/// </summary>
internal sealed class BTreeHandle : IDisposable
{
    private const int HEADER_SIZE = 32;
    private static readonly byte[] Magic = "SBTI"u8.ToArray();
    private const byte Version = 1;
    private const int MIN_ORDER = 4;
    private const int TARGET_NODE_BYTES = 4096;

    private readonly string _path;
    private readonly ColumnType _type;
    private readonly int _keySize;
    private readonly int _order;
    private readonly int _nodeSize;
    private readonly int _keyEntrySize; // keySize + 8 (place)

    private FileStream _fs;
    private MemoryMappedFile _mmf;
    private MemoryMappedViewAccessor _view;
    private long _capacity;
    private long _nodeCount;
    private long _rootOffset;
    private long _freeNodeOffset;

    // ── Construction ─────────────────────────────────────

    private BTreeHandle(string path, ColumnType type, int keySize, int order,
        FileStream fs, MemoryMappedFile mmf, MemoryMappedViewAccessor view,
        long capacity, long rootOffset, long nodeCount, long freeNodeOffset)
    {
        _path = path;
        _type = type;
        _keySize = keySize;
        _order = order;
        _keyEntrySize = keySize + 8;
        // Node = 1 (flags) + 2 (key_count) + order * (keySize+8) + (order+1) * 8
        _nodeSize = 3 + order * _keyEntrySize + (order + 1) * 8;
        _fs = fs;
        _mmf = mmf;
        _view = view;
        _capacity = capacity;
        _rootOffset = rootOffset;
        _nodeCount = nodeCount;
        _freeNodeOffset = freeNodeOffset;
    }

    /// <summary>
    /// Opens an existing .btree file.
    /// </summary>
    public static BTreeHandle Open(string path, ColumnType type, int keySize)
    {
        var fs = new FileStream(path, FileMode.Open, FileAccess.ReadWrite, FileShare.ReadWrite);
        var capacity = fs.Length;
        var (mmf, view) = CreateMapping(fs, capacity);

        // Read header
        var magic = new byte[4];
        view.ReadArray(0, magic, 0, 4);
        if (!magic.AsSpan().SequenceEqual(Magic))
            throw new InvalidDataException("Invalid B-Tree file magic");

        var order = view.ReadUInt16(6);
        var rootOffset = view.ReadInt64(8);
        var nodeCount = view.ReadInt64(16);
        var freeNodeOffset = view.ReadInt64(24);

        return new BTreeHandle(path, type, keySize, order, fs, mmf, view,
            capacity, rootOffset, nodeCount, freeNodeOffset);
    }

    /// <summary>
    /// Creates a new empty .btree file.
    /// </summary>
    public static BTreeHandle Create(string path, ColumnType type, int keySize)
    {
        var order = CalculateOrder(keySize);
        var nodeSize = 3 + order * (keySize + 8) + (order + 1) * 8;
        var initialCapacity = HEADER_SIZE + (long)nodeSize * 64; // room for 64 nodes

        using (var createFs = File.Create(path))
        {
            createFs.SetLength(initialCapacity);
        }

        var fs = new FileStream(path, FileMode.Open, FileAccess.ReadWrite, FileShare.ReadWrite);
        var (mmf, view) = CreateMapping(fs, initialCapacity);

        // Write header
        view.WriteArray(0, Magic, 0, 4);
        view.Write(4, Version);
        view.Write(6, (ushort)order);
        view.Write(8, (long)0);  // root_offset = 0 (empty)
        view.Write(16, (long)0); // node_count = 0
        view.Write(24, (long)0); // free_node_offset = 0

        return new BTreeHandle(path, type, keySize, order, fs, mmf, view,
            initialCapacity, 0, 0, 0);
    }

    /// <summary>
    /// Builds a B-Tree from an existing column by scanning all used places.
    /// </summary>
    public static BTreeHandle BuildFromColumn(string path, ColumnHandle col, IndexHandle idx,
        ColumnType type, int keySize)
    {
        var handle = Create(path, type, keySize);

        idx.ForEachUsed((id, place) =>
        {
            if (col.IsNullAtPlace(place))
                return;

            var key = ReadKeyFromColumn(col, place, keySize);
            handle.Insert(key, place);
        });

        handle.Flush();
        return handle;
    }

    // ── Lookup ───────────────────────────────────────────

    /// <summary>
    /// Finds all places matching the exact key.
    /// </summary>
    public List<long> Lookup(byte[] key)
    {
        var results = new List<long>();
        if (_rootOffset == 0)
            return results;

        LookupInNode(_rootOffset, key, results);
        return results;
    }

    private void LookupInNode(long nodeOffset, byte[] key, List<long> results)
    {
        var isLeaf = (_view.ReadByte(nodeOffset) & 1) != 0;
        var keyCount = _view.ReadUInt16(nodeOffset + 1);
        var keysStart = nodeOffset + 3;

        int i = 0;
        while (i < keyCount)
        {
            var cmp = CompareKeyAt(keysStart + (long)i * _keyEntrySize, key);
            if (cmp == 0)
            {
                // Found — collect this and check for duplicates
                results.Add(_view.ReadInt64(keysStart + (long)i * _keyEntrySize + _keySize));
                // Continue scanning for more entries with same key
                for (int j = i + 1; j < keyCount; j++)
                {
                    if (CompareKeyAt(keysStart + (long)j * _keyEntrySize, key) == 0)
                        results.Add(_view.ReadInt64(keysStart + (long)j * _keyEntrySize + _keySize));
                    else
                        break;
                }
                // Also check left subtree for duplicates in internal nodes
                if (!isLeaf)
                {
                    var childOffset = ReadChildOffset(nodeOffset, keyCount, i);
                    if (childOffset > 0)
                        LookupInNode(childOffset, key, results);
                }
                return;
            }
            if (cmp > 0)
                break;
            i++;
        }

        if (!isLeaf)
        {
            var childOffset = ReadChildOffset(nodeOffset, keyCount, i);
            if (childOffset > 0)
                LookupInNode(childOffset, key, results);
        }
    }

    /// <summary>
    /// Finds all places where key is in range [low, high] inclusive.
    /// Pass null for low or high for open-ended ranges.
    /// </summary>
    public List<long> RangeLookup(byte[]? low, byte[]? high)
    {
        var results = new List<long>();
        if (_rootOffset == 0)
            return results;

        RangeInNode(_rootOffset, low, high, results);
        return results;
    }

    private void RangeInNode(long nodeOffset, byte[]? low, byte[]? high, List<long> results)
    {
        var isLeaf = (_view.ReadByte(nodeOffset) & 1) != 0;
        var keyCount = _view.ReadUInt16(nodeOffset + 1);
        var keysStart = nodeOffset + 3;

        for (int i = 0; i < keyCount; i++)
        {
            var keyOffset = keysStart + (long)i * _keyEntrySize;

            // Check if we need to descend into left child
            if (!isLeaf)
            {
                var childOffset = ReadChildOffset(nodeOffset, keyCount, i);
                if (childOffset > 0)
                {
                    // Only descend if there could be keys >= low
                    if (low is null || CompareKeyAt(keyOffset, low) >= 0)
                        RangeInNode(childOffset, low, high, results);
                    else
                    {
                        // Key at i is less than low; the left child could still have valid keys
                        // if some keys in the child are >= low
                        RangeInNode(childOffset, low, high, results);
                    }
                }
            }

            // Check this key against range
            if (low is not null && CompareKeyAt(keyOffset, low) < 0)
                continue;
            if (high is not null && CompareKeyAt(keyOffset, high) > 0)
                return; // Past the end, all remaining keys are larger

            results.Add(_view.ReadInt64(keyOffset + _keySize));
        }

        // Rightmost child
        if (!isLeaf)
        {
            var rightChild = ReadChildOffset(nodeOffset, keyCount, keyCount);
            if (rightChild > 0)
            {
                if (high is null || (keyCount > 0 && CompareKeyAt(keysStart + (long)(keyCount - 1) * _keyEntrySize, high) <= 0))
                    RangeInNode(rightChild, low, high, results);
            }
        }
    }

    // ── Insert ──────────────────────────────────────────

    public void Insert(byte[] key, long place)
    {
        if (_rootOffset == 0)
        {
            // Create root as leaf
            _rootOffset = AllocateNode(isLeaf: true);
            WriteHeader();
        }

        var result = InsertInNode(_rootOffset, key, place);

        if (result.Split)
        {
            // Root was split — create new root
            var newRoot = AllocateNode(isLeaf: false);
            var newRootKeysStart = newRoot + 3;

            // Write single key (the promoted key)
            WriteKeyEntry(newRootKeysStart, result.PromotedKey, result.PromotedPlace);
            _view.Write(newRoot + 1, (ushort)1); // key_count = 1

            // Write children
            WriteChildOffset(newRoot, 1, 0, _rootOffset);
            WriteChildOffset(newRoot, 1, 1, result.NewNodeOffset);

            _rootOffset = newRoot;
            WriteHeader();
        }
    }

    private InsertResult InsertInNode(long nodeOffset, byte[] key, long place)
    {
        var isLeaf = (_view.ReadByte(nodeOffset) & 1) != 0;
        var keyCount = _view.ReadUInt16(nodeOffset + 1);
        var keysStart = nodeOffset + 3;

        // Find insertion position
        int pos = 0;
        while (pos < keyCount)
        {
            var cmp = CompareKeyAt(keysStart + (long)pos * _keyEntrySize, key);
            if (cmp > 0)
                break;
            pos++;
        }

        if (isLeaf)
        {
            if (keyCount < _order)
            {
                // Room available — shift keys right and insert
                ShiftKeysRight(keysStart, pos, keyCount);
                WriteKeyEntry(keysStart + (long)pos * _keyEntrySize, key, place);
                _view.Write(nodeOffset + 1, (ushort)(keyCount + 1));
                return InsertResult.NoSplit;
            }
            else
            {
                // Leaf is full — split
                return SplitLeaf(nodeOffset, key, place, pos);
            }
        }
        else
        {
            // Internal node: descend into child
            var childOffset = ReadChildOffset(nodeOffset, keyCount, pos);
            var result = InsertInNode(childOffset, key, place);

            if (!result.Split)
                return InsertResult.NoSplit;

            // Child was split — insert promoted key here
            if (keyCount < _order)
            {
                ShiftKeysRight(keysStart, pos, keyCount);
                WriteKeyEntry(keysStart + (long)pos * _keyEntrySize, result.PromotedKey, result.PromotedPlace);

                // Shift child pointers right
                ShiftChildrenRight(nodeOffset, keyCount, pos + 1);
                WriteChildOffset(nodeOffset, keyCount + 1, pos + 1, result.NewNodeOffset);

                _view.Write(nodeOffset + 1, (ushort)(keyCount + 1));
                return InsertResult.NoSplit;
            }
            else
            {
                // Internal node is full — split it
                return SplitInternal(nodeOffset, result.PromotedKey, result.PromotedPlace,
                    result.NewNodeOffset, pos);
            }
        }
    }

    // ── Remove ──────────────────────────────────────────

    public void Remove(byte[] key, long place)
    {
        if (_rootOffset == 0)
            return;

        RemoveFromNode(_rootOffset, key, place);

        // If root is internal and has 0 keys, shrink tree
        var rootIsLeaf = (_view.ReadByte(_rootOffset) & 1) != 0;
        var rootKeyCount = _view.ReadUInt16(_rootOffset + 1);
        if (!rootIsLeaf && rootKeyCount == 0)
        {
            var onlyChild = ReadChildOffset(_rootOffset, 0, 0);
            FreeNode(_rootOffset);
            _rootOffset = onlyChild;
            WriteHeader();
        }
    }

    private bool RemoveFromNode(long nodeOffset, byte[] key, long place)
    {
        var isLeaf = (_view.ReadByte(nodeOffset) & 1) != 0;
        var keyCount = _view.ReadUInt16(nodeOffset + 1);
        var keysStart = nodeOffset + 3;

        // Find key position
        int pos = 0;
        int foundAt = -1;

        for (int i = 0; i < keyCount; i++)
        {
            var keyOff = keysStart + (long)i * _keyEntrySize;
            var cmp = CompareKeyAt(keyOff, key);
            if (cmp == 0 && _view.ReadInt64(keyOff + _keySize) == place)
            {
                foundAt = i;
                break;
            }
            if (cmp > 0)
            {
                pos = i;
                break;
            }
            pos = i + 1;
        }

        if (isLeaf)
        {
            if (foundAt < 0)
                return false;

            // Remove from leaf by shifting left
            ShiftKeysLeft(keysStart, foundAt, keyCount);
            _view.Write(nodeOffset + 1, (ushort)(keyCount - 1));
            return true;
        }

        if (foundAt >= 0)
        {
            // Key found in internal node — replace with predecessor from left child
            var leftChild = ReadChildOffset(nodeOffset, keyCount, foundAt);
            var predKey = new byte[_keySize];
            long predPlace;
            FindMax(leftChild, predKey, out predPlace);

            // Replace this key with predecessor
            WriteKeyEntry(keysStart + (long)foundAt * _keyEntrySize, predKey, predPlace);

            // Remove predecessor from left subtree
            RemoveFromNode(leftChild, predKey, predPlace);

            // Rebalance if needed
            RebalanceChild(nodeOffset, foundAt);
            return true;
        }

        // Key not found here — descend
        var child = ReadChildOffset(nodeOffset, keyCount, pos);
        var removed = RemoveFromNode(child, key, place);

        if (removed)
            RebalanceChild(nodeOffset, pos);

        return removed;
    }

    private void FindMax(long nodeOffset, byte[] keyOut, out long placeOut)
    {
        var isLeaf = (_view.ReadByte(nodeOffset) & 1) != 0;
        var keyCount = _view.ReadUInt16(nodeOffset + 1);

        if (isLeaf)
        {
            var lastKeyOff = nodeOffset + 3 + (long)(keyCount - 1) * _keyEntrySize;
            _view.ReadArray(lastKeyOff, keyOut, 0, _keySize);
            placeOut = _view.ReadInt64(lastKeyOff + _keySize);
            return;
        }

        var rightChild = ReadChildOffset(nodeOffset, keyCount, keyCount);
        FindMax(rightChild, keyOut, out placeOut);
    }

    private void RebalanceChild(long parentOffset, int childIndex)
    {
        var parentKeyCount = _view.ReadUInt16(parentOffset + 1);
        var childOffset = ReadChildOffset(parentOffset, parentKeyCount, childIndex);
        var childKeyCount = _view.ReadUInt16(childOffset + 1);
        var minKeys = (_order - 1) / 2;

        if (childKeyCount >= minKeys)
            return;

        // Try borrow from left sibling
        if (childIndex > 0)
        {
            var leftSibling = ReadChildOffset(parentOffset, parentKeyCount, childIndex - 1);
            var leftKeyCount = _view.ReadUInt16(leftSibling + 1);
            if (leftKeyCount > minKeys)
            {
                BorrowFromLeft(parentOffset, childIndex, childOffset, leftSibling);
                return;
            }
        }

        // Try borrow from right sibling
        if (childIndex < parentKeyCount)
        {
            var rightSibling = ReadChildOffset(parentOffset, parentKeyCount, childIndex + 1);
            var rightKeyCount = _view.ReadUInt16(rightSibling + 1);
            if (rightKeyCount > minKeys)
            {
                BorrowFromRight(parentOffset, childIndex, childOffset, rightSibling);
                return;
            }
        }

        // Merge with a sibling
        if (childIndex > 0)
            MergeNodes(parentOffset, childIndex - 1);
        else
            MergeNodes(parentOffset, childIndex);
    }

    private void BorrowFromLeft(long parent, int childIdx, long child, long leftSibling)
    {
        var parentKeysStart = parent + 3;
        var childKeysStart = child + 3;
        var childKeyCount = _view.ReadUInt16(child + 1);
        var leftKeysStart = leftSibling + 3;
        var leftKeyCount = _view.ReadUInt16(leftSibling + 1);
        var childIsLeaf = (_view.ReadByte(child) & 1) != 0;

        // Shift child keys right to make room
        ShiftKeysRight(childKeysStart, 0, childKeyCount);
        if (!childIsLeaf)
            ShiftChildrenRight(child, childKeyCount, 0);

        // Move parent key[childIdx-1] down to child[0]
        var parentKeyOff = parentKeysStart + (long)(childIdx - 1) * _keyEntrySize;
        CopyKeyEntry(parentKeyOff, childKeysStart);

        // Move left sibling's last key up to parent
        var leftLastKeyOff = leftKeysStart + (long)(leftKeyCount - 1) * _keyEntrySize;
        CopyKeyEntry(leftLastKeyOff, parentKeyOff);

        // Move left sibling's rightmost child to child[0]
        if (!childIsLeaf)
        {
            var leftChildPtr = ReadChildOffset(leftSibling, leftKeyCount, leftKeyCount);
            WriteChildOffset(child, childKeyCount + 1, 0, leftChildPtr);
        }

        _view.Write(child + 1, (ushort)(childKeyCount + 1));
        _view.Write(leftSibling + 1, (ushort)(leftKeyCount - 1));
    }

    private void BorrowFromRight(long parent, int childIdx, long child, long rightSibling)
    {
        var parentKeysStart = parent + 3;
        var childKeysStart = child + 3;
        var childKeyCount = _view.ReadUInt16(child + 1);
        var rightKeysStart = rightSibling + 3;
        var rightKeyCount = _view.ReadUInt16(rightSibling + 1);
        var childIsLeaf = (_view.ReadByte(child) & 1) != 0;

        // Append parent key[childIdx] to end of child
        var parentKeyOff = parentKeysStart + (long)childIdx * _keyEntrySize;
        CopyKeyEntry(parentKeyOff, childKeysStart + (long)childKeyCount * _keyEntrySize);

        // Move right sibling's first child to child's new rightmost child
        if (!childIsLeaf)
        {
            var rightFirstChild = ReadChildOffset(rightSibling, rightKeyCount, 0);
            WriteChildOffset(child, childKeyCount + 1, childKeyCount + 1, rightFirstChild);
        }

        // Move right sibling's first key up to parent
        CopyKeyEntry(rightKeysStart, parentKeyOff);

        // Shift right sibling's keys left
        ShiftKeysLeft(rightKeysStart, 0, rightKeyCount);
        if (!childIsLeaf)
            ShiftChildrenLeft(rightSibling, rightKeyCount, 0);

        _view.Write(child + 1, (ushort)(childKeyCount + 1));
        _view.Write(rightSibling + 1, (ushort)(rightKeyCount - 1));
    }

    private void MergeNodes(long parent, int leftIdx)
    {
        var parentKeyCount = _view.ReadUInt16(parent + 1);
        var parentKeysStart = parent + 3;
        var leftChild = ReadChildOffset(parent, parentKeyCount, leftIdx);
        var rightChild = ReadChildOffset(parent, parentKeyCount, leftIdx + 1);
        var leftKeyCount = _view.ReadUInt16(leftChild + 1);
        var rightKeyCount = _view.ReadUInt16(rightChild + 1);
        var leftKeysStart = leftChild + 3;
        var rightKeysStart = rightChild + 3;
        var leftIsLeaf = (_view.ReadByte(leftChild) & 1) != 0;

        // Move parent key down to left child
        var parentKeyOff = parentKeysStart + (long)leftIdx * _keyEntrySize;
        CopyKeyEntry(parentKeyOff, leftKeysStart + (long)leftKeyCount * _keyEntrySize);
        int mergedCount = leftKeyCount + 1;

        // Copy all right sibling keys to left
        for (int i = 0; i < rightKeyCount; i++)
        {
            CopyKeyEntry(rightKeysStart + (long)i * _keyEntrySize,
                leftKeysStart + (long)mergedCount * _keyEntrySize);
            mergedCount++;
        }

        // Copy right sibling's children
        if (!leftIsLeaf)
        {
            for (int i = 0; i <= rightKeyCount; i++)
            {
                var childPtr = ReadChildOffset(rightChild, rightKeyCount, i);
                WriteChildOffset(leftChild, mergedCount, leftKeyCount + 1 + i, childPtr);
            }
        }

        _view.Write(leftChild + 1, (ushort)mergedCount);

        // Remove parent key and right child pointer
        ShiftKeysLeft(parentKeysStart, leftIdx, parentKeyCount);
        ShiftChildrenLeft(parent, parentKeyCount, leftIdx + 1);
        _view.Write(parent + 1, (ushort)(parentKeyCount - 1));

        FreeNode(rightChild);
    }

    // ── Split helpers ────────────────────────────────────

    private InsertResult SplitLeaf(long nodeOffset, byte[] key, long place, int insertPos)
    {
        var keysStart = nodeOffset + 3;
        var newNode = AllocateNode(isLeaf: true);
        var newKeysStart = newNode + 3;

        // Collect all keys + the new one
        var totalKeys = _order + 1;
        var allKeys = new byte[totalKeys * _keyEntrySize];

        // Copy existing keys
        for (int i = 0; i < _order; i++)
        {
            var src = keysStart + (long)i * _keyEntrySize;
            var dst = (i < insertPos ? i : i + 1) * _keyEntrySize;
            _view.ReadArray(src, allKeys, dst, _keyEntrySize);
        }
        // Insert new key
        Array.Copy(key, 0, allKeys, insertPos * _keyEntrySize, _keySize);
        BinaryPrimitives.WriteInt64LittleEndian(
            allKeys.AsSpan(insertPos * _keyEntrySize + _keySize), place);

        var mid = totalKeys / 2;

        // Write left half to original node
        for (int i = 0; i < mid; i++)
            _view.WriteArray(keysStart + (long)i * _keyEntrySize, allKeys,
                i * _keyEntrySize, _keyEntrySize);
        _view.Write(nodeOffset + 1, (ushort)mid);

        // Write right half to new node
        var rightCount = totalKeys - mid;
        for (int i = 0; i < rightCount; i++)
            _view.WriteArray(newKeysStart + (long)i * _keyEntrySize, allKeys,
                (mid + i) * _keyEntrySize, _keyEntrySize);
        _view.Write(newNode + 1, (ushort)rightCount);

        // Promoted key is the first key of the right half
        var promotedKey = new byte[_keySize];
        Array.Copy(allKeys, mid * _keyEntrySize, promotedKey, 0, _keySize);
        var promotedPlace = BinaryPrimitives.ReadInt64LittleEndian(
            allKeys.AsSpan(mid * _keyEntrySize + _keySize));

        return new InsertResult(true, promotedKey, promotedPlace, newNode);
    }

    private InsertResult SplitInternal(long nodeOffset, byte[] key, long place,
        long newChildOffset, int insertPos)
    {
        var keysStart = nodeOffset + 3;
        var keyCount = _view.ReadUInt16(nodeOffset + 1);
        var newNode = AllocateNode(isLeaf: false);
        var newKeysStart = newNode + 3;

        // Collect all keys + the new promoted one
        var totalKeys = keyCount + 1;
        var allKeys = new byte[totalKeys * _keyEntrySize];
        var allChildren = new long[totalKeys + 1];

        // Copy existing keys
        for (int i = 0; i < keyCount; i++)
        {
            var src = keysStart + (long)i * _keyEntrySize;
            var dst = (i < insertPos ? i : i + 1) * _keyEntrySize;
            _view.ReadArray(src, allKeys, dst, _keyEntrySize);
        }
        Array.Copy(key, 0, allKeys, insertPos * _keyEntrySize, _keySize);
        BinaryPrimitives.WriteInt64LittleEndian(
            allKeys.AsSpan(insertPos * _keyEntrySize + _keySize), place);

        // Copy existing children
        for (int i = 0; i <= keyCount; i++)
        {
            var ci = i < insertPos + 1 ? i : i + 1;
            allChildren[ci] = ReadChildOffset(nodeOffset, keyCount, i);
        }
        allChildren[insertPos + 1] = newChildOffset;

        var mid = totalKeys / 2;

        // Left half stays in original
        for (int i = 0; i < mid; i++)
            _view.WriteArray(keysStart + (long)i * _keyEntrySize, allKeys,
                i * _keyEntrySize, _keyEntrySize);
        for (int i = 0; i <= mid; i++)
            WriteChildOffset(nodeOffset, mid, i, allChildren[i]);
        _view.Write(nodeOffset + 1, (ushort)mid);

        // Right half goes to new node
        var rightCount = totalKeys - mid - 1; // -1 because mid key is promoted
        for (int i = 0; i < rightCount; i++)
            _view.WriteArray(newKeysStart + (long)i * _keyEntrySize, allKeys,
                (mid + 1 + i) * _keyEntrySize, _keyEntrySize);
        for (int i = 0; i <= rightCount; i++)
            WriteChildOffset(newNode, rightCount, i, allChildren[mid + 1 + i]);
        _view.Write(newNode + 1, (ushort)rightCount);

        // Promoted key is the middle
        var promotedKey = new byte[_keySize];
        Array.Copy(allKeys, mid * _keyEntrySize, promotedKey, 0, _keySize);
        var promotedPlace = BinaryPrimitives.ReadInt64LittleEndian(
            allKeys.AsSpan(mid * _keyEntrySize + _keySize));

        return new InsertResult(true, promotedKey, promotedPlace, newNode);
    }

    // ── Node allocation ─────────────────────────────────

    private long AllocateNode(bool isLeaf)
    {
        long offset;

        if (_freeNodeOffset > 0)
        {
            offset = _freeNodeOffset;
            // Read next free from the freed node's first 8 bytes
            _freeNodeOffset = _view.ReadInt64(offset);
            WriteHeader();
        }
        else
        {
            offset = HEADER_SIZE + _nodeCount * _nodeSize;
            _nodeCount++;

            // Ensure capacity
            var required = offset + _nodeSize;
            if (required > _capacity)
            {
                var newCapacity = _capacity;
                while (newCapacity < required)
                    newCapacity += (long)_nodeSize * 64;
                Remap(newCapacity);
            }

            WriteHeader();
        }

        // Zero the node
        var zeroBuf = new byte[_nodeSize];
        _view.WriteArray(offset, zeroBuf, 0, _nodeSize);

        // Set leaf flag
        _view.Write(offset, (byte)(isLeaf ? 1 : 0));

        return offset;
    }

    private void FreeNode(long nodeOffset)
    {
        // Use freed node's space to store pointer to next free node (simple free list)
        _view.Write(nodeOffset, (byte)0); // clear flags
        _view.Write(nodeOffset + 1, (ushort)0); // clear key count
        // Store current free list head in the node
        var zeroBuf = new byte[_nodeSize - 8];
        _view.WriteArray(nodeOffset + 8, zeroBuf, 0, zeroBuf.Length);
        _view.Write(nodeOffset, _freeNodeOffset); // reuse first 8 bytes as next-free pointer
        _freeNodeOffset = nodeOffset;
        WriteHeader();
    }

    // ── Key comparison ──────────────────────────────────

    private int CompareKeyAt(long offset, byte[] key)
    {
        // Read stored key bytes
        var stored = new byte[_keySize];
        _view.ReadArray(offset, stored, 0, _keySize);
        return CompareKeys(stored, key);
    }

    private int CompareKeys(byte[] a, byte[] b)
    {
        return _type switch
        {
            ColumnType.Bool => a[0].CompareTo(b[0]),
            ColumnType.UByte => a[0].CompareTo(b[0]),
            ColumnType.SByte => ((sbyte)a[0]).CompareTo((sbyte)b[0]),
            ColumnType.UShort => BinaryPrimitives.ReadUInt16LittleEndian(a)
                .CompareTo(BinaryPrimitives.ReadUInt16LittleEndian(b)),
            ColumnType.SShort => BinaryPrimitives.ReadInt16LittleEndian(a)
                .CompareTo(BinaryPrimitives.ReadInt16LittleEndian(b)),
            ColumnType.UInt => BinaryPrimitives.ReadUInt32LittleEndian(a)
                .CompareTo(BinaryPrimitives.ReadUInt32LittleEndian(b)),
            ColumnType.SInt => BinaryPrimitives.ReadInt32LittleEndian(a)
                .CompareTo(BinaryPrimitives.ReadInt32LittleEndian(b)),
            ColumnType.ULong => BinaryPrimitives.ReadUInt64LittleEndian(a)
                .CompareTo(BinaryPrimitives.ReadUInt64LittleEndian(b)),
            ColumnType.SLong => BinaryPrimitives.ReadInt64LittleEndian(a)
                .CompareTo(BinaryPrimitives.ReadInt64LittleEndian(b)),
            ColumnType.Float => BinaryPrimitives.ReadSingleLittleEndian(a)
                .CompareTo(BinaryPrimitives.ReadSingleLittleEndian(b)),
            ColumnType.Double => BinaryPrimitives.ReadDoubleLittleEndian(a)
                .CompareTo(BinaryPrimitives.ReadDoubleLittleEndian(b)),
            ColumnType.Date => BinaryPrimitives.ReadInt32LittleEndian(a)
                .CompareTo(BinaryPrimitives.ReadInt32LittleEndian(b)),
            ColumnType.Time => BinaryPrimitives.ReadInt64LittleEndian(a)
                .CompareTo(BinaryPrimitives.ReadInt64LittleEndian(b)),
            ColumnType.DateTime => BinaryPrimitives.ReadInt64LittleEndian(a)
                .CompareTo(BinaryPrimitives.ReadInt64LittleEndian(b)),
            ColumnType.String => CompareStringKeys(a, b),
            _ => a.AsSpan().SequenceCompareTo(b),
        };
    }

    private static int CompareStringKeys(byte[] a, byte[] b)
    {
        for (int i = 0; i < a.Length; i++)
        {
            var cmp = a[i].CompareTo(b[i]);
            if (cmp != 0) return cmp;
        }
        return 0;
    }

    // ── Helpers ──────────────────────────────────────────

    private void WriteKeyEntry(long offset, byte[] key, long place)
    {
        _view.WriteArray(offset, key, 0, _keySize);
        _view.Write(offset + _keySize, place);
    }

    private void CopyKeyEntry(long srcOffset, long dstOffset)
    {
        var buf = new byte[_keyEntrySize];
        _view.ReadArray(srcOffset, buf, 0, _keyEntrySize);
        _view.WriteArray(dstOffset, buf, 0, _keyEntrySize);
    }

    private void ShiftKeysRight(long keysStart, int fromIndex, int count)
    {
        for (int i = count - 1; i >= fromIndex; i--)
        {
            CopyKeyEntry(
                keysStart + (long)i * _keyEntrySize,
                keysStart + (long)(i + 1) * _keyEntrySize);
        }
    }

    private void ShiftKeysLeft(long keysStart, int fromIndex, int count)
    {
        for (int i = fromIndex; i < count - 1; i++)
        {
            CopyKeyEntry(
                keysStart + (long)(i + 1) * _keyEntrySize,
                keysStart + (long)i * _keyEntrySize);
        }
    }

    private long ChildrenStart(long nodeOffset, int keyCount)
    {
        return nodeOffset + 3 + (long)_order * _keyEntrySize;
    }

    private long ReadChildOffset(long nodeOffset, int keyCount, int childIndex)
    {
        var childrenStart = ChildrenStart(nodeOffset, keyCount);
        return _view.ReadInt64(childrenStart + (long)childIndex * 8);
    }

    private void WriteChildOffset(long nodeOffset, int keyCount, int childIndex, long childOffset)
    {
        var childrenStart = ChildrenStart(nodeOffset, keyCount);
        _view.Write(childrenStart + (long)childIndex * 8, childOffset);
    }

    private void ShiftChildrenRight(long nodeOffset, int keyCount, int fromIndex)
    {
        var childrenStart = ChildrenStart(nodeOffset, keyCount);
        for (int i = keyCount; i >= fromIndex; i--)
        {
            var val = _view.ReadInt64(childrenStart + (long)i * 8);
            _view.Write(childrenStart + (long)(i + 1) * 8, val);
        }
    }

    private void ShiftChildrenLeft(long nodeOffset, int keyCount, int fromIndex)
    {
        var childrenStart = ChildrenStart(nodeOffset, keyCount);
        for (int i = fromIndex; i < keyCount; i++)
        {
            var val = _view.ReadInt64(childrenStart + (long)(i + 1) * 8);
            _view.Write(childrenStart + (long)i * 8, val);
        }
    }

    private static byte[] ReadKeyFromColumn(ColumnHandle col, long place, int keySize)
    {
        var buf = new byte[keySize];
        // Read raw bytes from column at the value offset (skip flag byte)
        // We use EncodeValueToBytes pattern but read from col directly
        var value = col.ReadValue(place);
        if (value is null) return buf;

        var raw = value.ToString() ?? "";
        return col.EncodeValueToBytes(raw);
    }

    private void WriteHeader()
    {
        _view.Write(8, _rootOffset);
        _view.Write(16, _nodeCount);
        _view.Write(24, _freeNodeOffset);
    }

    private static int CalculateOrder(int keySize)
    {
        // Node: 3 bytes header + order * (keySize+8) + (order+1) * 8
        // Solve: 3 + order * (keySize + 8) + (order + 1) * 8 <= TARGET
        // 3 + order * keySize + 8*order + 8*order + 8 <= TARGET
        // order * (keySize + 16) <= TARGET - 11
        var order = (TARGET_NODE_BYTES - 11) / (keySize + 16);
        return Math.Max(order, MIN_ORDER);
    }

    // ── MMF lifecycle ───────────────────────────────────

    public void Flush()
    {
        _view.Flush();
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
        Flush();
        _view.Dispose();
        _mmf.Dispose();
        _fs.Dispose();
    }

    // ── Internal types ──────────────────────────────────

    private readonly struct InsertResult
    {
        public static readonly InsertResult NoSplit = new(false, Array.Empty<byte>(), 0, 0);

        public readonly bool Split;
        public readonly byte[] PromotedKey;
        public readonly long PromotedPlace;
        public readonly long NewNodeOffset;

        public InsertResult(bool split, byte[] promotedKey, long promotedPlace, long newNodeOffset)
        {
            Split = split;
            PromotedKey = promotedKey;
            PromotedPlace = promotedPlace;
            NewNodeOffset = newNodeOffset;
        }
    }
}

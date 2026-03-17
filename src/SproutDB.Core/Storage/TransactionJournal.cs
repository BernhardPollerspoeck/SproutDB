namespace SproutDB.Core.Storage;

/// <summary>
/// Records all MMF-level changes during a transaction.
/// On rollback, replays undo entries in reverse order to restore the previous state.
/// </summary>
internal sealed class TransactionJournal
{
    private readonly List<IUndoEntry> _entries = [];

    public void RecordColumnWrite(ColumnHandle col, long place)
    {
        var offset = place * col.Schema.EntrySize;
        var buf = new byte[col.Schema.EntrySize];
        col.ReadRawEntry(place, buf);
        _entries.Add(new ColumnUndoEntry(col, place, buf));
    }

    public void RecordIndexWritePlace(IndexHandle idx, long place)
    {
        var oldId = idx.FindIdForPlace(place);
        var oldCount = idx.ActiveRowCount;
        var oldNextId = idx.ReadNextId();
        _entries.Add(new IndexWritePlaceUndoEntry(idx, place, oldId, oldCount, oldNextId));
    }

    public void RecordIndexFreeSlot(IndexHandle idx, long place)
    {
        var oldId = idx.FindIdForPlace(place);
        var oldCount = idx.ActiveRowCount;
        _entries.Add(new IndexFreeSlotUndoEntry(idx, place, oldId, oldCount));
    }

    public void RecordIndexWriteNextId(IndexHandle idx)
    {
        var oldNextId = idx.ReadNextId();
        _entries.Add(new IndexNextIdUndoEntry(idx, oldNextId));
    }

    public void RecordBTreeInsert(BTreeHandle btree, byte[] key, long place)
    {
        _entries.Add(new BTreeInsertUndoEntry(btree, key, place));
    }

    public void RecordBTreeRemove(BTreeHandle btree, byte[] key, long place)
    {
        _entries.Add(new BTreeRemoveUndoEntry(btree, key, place));
    }

    /// <summary>
    /// Reverses all recorded changes in reverse order.
    /// </summary>
    public void Rollback()
    {
        for (var i = _entries.Count - 1; i >= 0; i--)
            _entries[i].Undo();
    }

    // ── Undo entry types ────────────────────────────────────

    private interface IUndoEntry
    {
        void Undo();
    }

    private sealed class ColumnUndoEntry(ColumnHandle col, long place, byte[] oldData) : IUndoEntry
    {
        public void Undo() => col.WriteRawEntry(place, oldData);
    }

    private sealed class IndexWritePlaceUndoEntry(
        IndexHandle idx, long place, ulong oldId, long oldCount, ulong oldNextId) : IUndoEntry
    {
        public void Undo()
        {
            // Restore the slot to its previous state
            if (oldId == 0)
            {
                // Slot was empty before — free it
                var currentId = idx.FindIdForPlace(place);
                if (currentId != 0)
                    idx.FreeSlot(place);
            }

            // Restore count and nextId
            idx.RestoreState(oldCount, oldNextId);
        }
    }

    private sealed class IndexFreeSlotUndoEntry(
        IndexHandle idx, long place, ulong oldId, long oldCount) : IUndoEntry
    {
        public void Undo()
        {
            // Slot was occupied before — restore it
            if (oldId != 0)
                idx.WritePlace(oldId, place);

            // Restore count (WritePlace increments, so we fix)
            idx.RestoreCount(oldCount);
        }
    }

    private sealed class IndexNextIdUndoEntry(IndexHandle idx, ulong oldNextId) : IUndoEntry
    {
        public void Undo() => idx.WriteNextId(oldNextId);
    }

    private sealed class BTreeInsertUndoEntry(BTreeHandle btree, byte[] key, long place) : IUndoEntry
    {
        // Undo an insert = remove
        public void Undo() => btree.Remove(key, place);
    }

    private sealed class BTreeRemoveUndoEntry(BTreeHandle btree, byte[] key, long place) : IUndoEntry
    {
        // Undo a remove = re-insert
        public void Undo() => btree.Insert(key, place);
    }
}

using SproutDB.Core.Parsing;
using SproutDB.Core.Storage;

namespace SproutDB.Core.Execution;

internal static class DeleteExecutor
{
    /// <param name="beforeWrite">
    /// Invoked right before the first row is deleted (after validation and the
    /// <c>expect</c> check) — the engine appends the WAL entry here. Not invoked
    /// when nothing matches or the delete is rejected.
    /// </param>
    public static SproutResponse Execute(string query, TableHandle table, DeleteQuery q,
        Storage.TransactionJournal? journal = null, Action? beforeWrite = null)
    {
        // Validate where tree
        var whereErrors = WhereEngine.ValidateWhereNode(table, q.Where);
        if (whereErrors is not null)
            return ResponseHelper.Errors(query, whereErrors);

        // Prepare compiled filter
        var filter = WhereEngine.PrepareFilter(table, q.Where);

        // With 'expect', expired (TTL) rows count as not existing: they are neither
        // counted nor deleted — the TTL cleanup removes them.
        var ttl = q.Expect is not null ? table.Ttl : null;
        var nowMs = ttl is not null ? DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() : 0L;

        // Collect matching rows first, then delete (safe slot-based iteration)
        var toDelete = new List<(ulong Id, long Place)>();
        if (filter is not null)
        {
            table.Index.ForEachUsed((id, place) =>
            {
                if (ttl is not null)
                {
                    var expiresAt = ttl.ReadExpiresAt(place);
                    if (expiresAt > 0 && nowMs > expiresAt)
                        return;
                }

                if (WhereEngine.EvaluateFilter(filter, id, place))
                    toDelete.Add((id, place));
            });
        }

        if (q.Expect is { } expected && !q.IsReplay && toDelete.Count != expected)
        {
            return ResponseHelper.Error(query, ErrorCodes.EXPECTATION_FAILED,
                $"expected {expected} matching row(s), found {toDelete.Count} — nothing deleted");
        }

        if (toDelete.Count == 0)
            return new SproutResponse { Operation = SproutOperation.Delete, Affected = 0 };

        beforeWrite?.Invoke();

        foreach (var (id, place) in toDelete)
            DeleteRow(table, id, place, journal);

        return new SproutResponse { Operation = SproutOperation.Delete, Affected = toDelete.Count };
    }

    /// <summary>
    /// Physically removes one row: B-Tree entries, blob/array files, slot, TTL, column data.
    /// </summary>
    internal static void DeleteRow(TableHandle table, ulong id, long place, Storage.TransactionJournal? journal)
    {
        // Remove from B-Trees before clearing data
        foreach (var col in table.Schema.Columns)
        {
            if (table.HasBTree(col.Name))
            {
                var colHandle = table.GetColumn(col.Name);
                if (!colHandle.IsNullAtPlace(place))
                {
                    var val = colHandle.ReadValue(place);
                    if (val is not null)
                    {
                        var encoded = colHandle.EncodeValueToBytes(val.ToString() ?? "");
                        table.GetBTree(col.Name).Remove(encoded, place, journal);
                    }
                }
            }
        }

        // Delete blob/array files
        foreach (var col in table.Schema.Columns)
        {
            ColumnTypes.TryParse(col.Type, out var colType);
            if (colType == ColumnType.Blob)
                table.DeleteBlobFile(col.Name, (long)id, journal);
            else if (colType == ColumnType.Array)
                table.DeleteArrayFile(col.Name, (long)id, journal);
        }

        // Free slot (marks as deleted, decrements count)
        table.Index.FreeSlot(place, journal);

        // Clear TTL entry
        table.Ttl?.Clear(place, journal);

        // Write null flag for each column
        foreach (var col in table.Schema.Columns)
            table.GetColumn(col.Name).WriteNull(place, journal);
    }
}

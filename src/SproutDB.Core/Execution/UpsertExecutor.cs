using SproutDB.Core.Parsing;
using SproutDB.Core.Storage;

namespace SproutDB.Core.Execution;

internal static class UpsertExecutor
{
    /// <param name="beforeWrite">
    /// Invoked once all validation (incl. the <c>when</c> condition) has passed and
    /// right before the first mutation — the engine appends the WAL entry here, so
    /// rejected upserts never reach the WAL. The argument is the _id a single-record
    /// upsert is about to write (0 for bulk), stored in the WAL for replay.
    /// </param>
    public static SproutResponse Execute(string query, TableHandle table, UpsertQuery q, int bulkLimit,
        Storage.TransactionJournal? journal = null, Action<ulong>? beforeWrite = null)
    {
        // ── Bulk limit check (fail fast, before any validation/writes) ──
        if (q.Records.Count > bulkLimit)
            return ResponseHelper.Error(query, ErrorCodes.BULK_LIMIT,
                $"bulk upsert exceeds limit of {bulkLimit} records ({q.Records.Count} given)");

        // ── TTL validation: row TTL requires table to have TTL enabled ──
        bool hasTtl = table.HasTtl;
        for (int i = 0; i < q.RowTtlSeconds.Count; i++)
        {
            if (q.RowTtlSeconds[i] > 0 && !hasTtl)
            {
                // Auto-enable TTL file if row has TTL but table doesn't have _ttl file yet
                table.EnableTtl();
                hasTtl = true;
                break;
            }
        }

        // ── Validate all records upfront ─────────────────────────────────
        var parsed = new ParsedRecord[q.Records.Count];
        for (int i = 0; i < q.Records.Count; i++)
        {
            var result = ValidateRecord(query, table, q.Records[i]);
            if (result.Error is not null)
                return result.Error;
            parsed[i] = result;
        }

        // ── Explicit _id existence check (a 'when' condition reports a missing row itself) ──
        for (int i = 0; i < parsed.Length && q.When is null; i++)
        {
            var explicitId = parsed[i].ExplicitId;
            if (explicitId.HasValue)
            {
                var existingPlace = table.Index.ReadPlace(explicitId.Value);
                if (existingPlace < 0)
                    return ResponseHelper.Error(query, ErrorCodes.ID_NOT_FOUND,
                        $"row with _id {explicitId.Value} does not exist");
            }
        }

        // ── ON clause validation ─────────────────────────────────────────
        if (q.OnColumn is not null)
        {
            if (!table.HasColumn(q.OnColumn))
                return ResponseHelper.Error(query, ErrorCodes.UNKNOWN_COLUMN,
                    $"on column '{q.OnColumn}' does not exist");

            for (int i = 0; i < parsed.Length; i++)
            {
                if (parsed[i].ExplicitId.HasValue)
                    return ResponseHelper.Error(query, ErrorCodes.SYNTAX_ERROR,
                        "cannot combine explicit _id with 'on' clause");

                if (!parsed[i].FieldsByName.ContainsKey(q.OnColumn))
                    return ResponseHelper.Error(query, ErrorCodes.SYNTAX_ERROR,
                        $"on column '{q.OnColumn}' must be included in the upsert fields");
            }

            // Single-pass scan: collect all match values, scan once, resolve IDs
            ResolveOnColumnIds(table, q.OnColumn, parsed);
        }

        // ── WHEN condition (single record, evaluated on the writer thread) ──
        if (q.When is { } when)
        {
            var conditionError = CheckCondition(query, table, when, parsed[0], q.IsReplay);
            if (conditionError is not null)
                return conditionError;
        }

        // ── Expired rows found via 'on' count as not existing ────────────
        // Such a row is freed (physically deleted) and the record inserted as a
        // genuinely new row — as if the TTL cleanup had already run.
        // On replay, q.ReplayId is the _id the write actually produced. A found row
        // with exactly that _id is the write's own row (update it); a found row with
        // another _id was freed by the original write — free it again. Expiry is not
        // re-checked there: replayed TTLs restart at replay time, so a row that was
        // expired live can look alive now.
        var fresh = false;
        for (int i = 0; i < parsed.Length; i++)
        {
            if (parsed[i].ResolvedOnId is not { } foundId)
                continue;

            if (q.IsReplay && q.ReplayId is { } producedId)
            {
                if (producedId == foundId)
                    fresh = q.When?.Kind == UpsertConditionKind.NotExists; // own insert already on disk
                else
                    parsed[i].ExpiredId = foundId;
                continue;
            }

            var foundPlace = parsed[i].ResolvedOnPlace ?? table.Index.ReadPlace(foundId);
            if (foundPlace >= 0 && IsExpired(table, foundPlace))
                parsed[i].ExpiredId = foundId;
        }

        // ── WAL replay: an insert gets the _id it was originally assigned ──
        if (q.ReplayId is { } replayId && parsed.Length == 1
            && parsed[0].ExplicitId is null && parsed[0].ResolvedOnId is null)
        {
            parsed[0].ReplayId = replayId;
        }
        else if (q.IsReplay && q.ReplayId is { } firstId && parsed.Length > 1 && q.OnColumn is null)
        {
            // Bulk insert: auto-ID records got firstId, firstId+1, … in record order.
            // Reusing them makes the replay idempotent (rows already on disk are updated).
            var nextId = firstId;
            for (int i = 0; i < parsed.Length; i++)
            {
                if (parsed[i].ExplicitId is null)
                    parsed[i].ReplayId = nextId++;
            }
        }

        // ── Unique constraint validation ───────────────────────────────────
        // (An expired row still counts as the record's own row here — it is freed below.)
        var uniqueError = ValidateUniqueConstraints(query, table, parsed, q.OnColumn);
        if (uniqueError is not null)
            return uniqueError;

        beforeWrite?.Invoke(WalIdFor(table, parsed, q.OnColumn));

        // ── Free expired rows, their records become inserts ──────────────
        for (int i = 0; i < parsed.Length; i++)
        {
            if (parsed[i].ExpiredId is not { } staleId)
                continue;

            var stalePlace = table.Index.ReadPlace(staleId);
            if (stalePlace >= 0)
                DeleteExecutor.DeleteRow(table, staleId, stalePlace, journal);

            parsed[i].ResolvedOnId = null;
            if (parsed.Length == 1)
                parsed[i].ReplayId = q.ReplayId; // replay: same _id as the original insert
        }

        // ── Execute all records ──────────────────────────────────────────
        var nowMs = hasTtl ? DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() : 0L;
        var tableTtlSeconds = table.Schema.TtlSeconds;

        var data = new List<Dictionary<string, object?>>(parsed.Length);
        for (int i = 0; i < parsed.Length; i++)
        {
            var rec = parsed[i];
            var resolvedId = rec.ExplicitId ?? rec.ResolvedOnId ?? rec.ReplayId;
            Dictionary<string, object?> record;
            ulong id;
            long place;

            if (resolvedId.HasValue)
            {
                // A row matched via 'on' has a known slot — spares the O(n) ReadPlace scan
                var knownPlace = rec.ExplicitId is null && rec.ResolvedOnId is not null ? rec.ResolvedOnPlace : null;
                (record, id, place) = WriteWithId(table, rec.FieldsByName, resolvedId.Value, journal, fresh, knownPlace);
            }
            else
            {
                (record, id, place) = WriteInsert(table, rec.FieldsByName, journal);
            }

            // Write TTL data
            if (hasTtl)
            {
                var rowTtl = q.RowTtlSeconds[i];
                var effectiveTtl = rowTtl > 0 ? rowTtl : tableTtlSeconds;
                var expiresAt = effectiveTtl > 0 ? nowMs + effectiveTtl * 1000 : 0L;
                table.Ttl?.Write(place, expiresAt, rowTtl, journal);
            }

            record["_id"] = id;
            data.Add(record);
        }

        return new SproutResponse
        {
            Operation = SproutOperation.Upsert,
            Data = data,
            Affected = data.Count,
        };
    }

    /// <summary>
    /// The _id stored in the WAL entry. Single record: the _id it will write — the
    /// explicit / matched row, or for an insert (incl. one replacing an expired row)
    /// the next auto-ID. Bulk without 'on': the first auto-ID; the auto-ID records
    /// get it and its successors in record order, so replay can hand out the same
    /// IDs again. Bulk with 'on': 0 (replay resolves rows via 'on').
    /// </summary>
    private static ulong WalIdFor(TableHandle table, ParsedRecord[] parsed, string? onColumn)
    {
        if (parsed.Length != 1)
            return onColumn is null ? table.Index.ReadNextId() : 0;

        var rec = parsed[0];
        if (rec.ExplicitId is { } explicitId)
            return explicitId;
        if (rec.ExpiredId is null && rec.ResolvedOnId is { } matchedId)
            return matchedId;
        return table.Index.ReadNextId();
    }

    // ── WHEN condition ───────────────────────────────────────────────

    /// <summary>
    /// Evaluates the <c>when</c> clause against the stored row found via <c>on</c> / <c>_id</c>.
    /// An expired (TTL) row counts as not existing. Returns CONDITION_FAILED with the
    /// current row in <c>Data</c> (empty if there is none), or null if the write may proceed.
    /// On replay only the expression is validated — the condition held when the write ran.
    /// </summary>
    private static SproutResponse? CheckCondition(
        string query, TableHandle table, UpsertCondition when, ParsedRecord rec, bool isReplay)
    {
        if (when.Where is { } where)
        {
            var whereErrors = WhereEngine.ValidateWhereNode(table, where);
            if (whereErrors is not null)
                return ResponseHelper.Errors(query, whereErrors);
        }

        if (isReplay)
            return null;

        var targetId = rec.ExplicitId ?? rec.ResolvedOnId;
        var place = rec.ExplicitId is null && rec.ResolvedOnPlace is { } knownPlace
            ? knownPlace
            : targetId is { } id ? table.Index.ReadPlace(id) : -1;
        var exists = place >= 0 && !IsExpired(table, place);

        switch (when.Kind)
        {
            case UpsertConditionKind.NotExists:
                return exists && targetId is { } existingId
                    ? ConditionFailed(query, when, "row already exists", GetExecutor.ReadFullRow(table, existingId, place))
                    : null;

            case UpsertConditionKind.Exists:
                return exists ? null : ConditionFailed(query, when, "row does not exist", null);

            default:
                if (!exists || targetId is not { } rowId)
                    return ConditionFailed(query, when, "row does not exist", null);

                var filter = WhereEngine.PrepareFilter(table, when.Where);
                if (filter is not null && WhereEngine.EvaluateFilter(filter, rowId, place))
                    return null;

                return ConditionFailed(query, when, "'when' condition is false for the existing row",
                    GetExecutor.ReadFullRow(table, rowId, place));
        }
    }

    /// <summary>
    /// Reads the stored row a single-record upsert targets (via <c>_id</c> or <c>on</c>),
    /// as <c>get</c> would return it. Null if there is none or it is expired.
    /// Used to report the committed row after a transaction was rolled back.
    /// </summary>
    internal static Dictionary<string, object?>? ReadCurrentRow(TableHandle table, UpsertQuery q)
    {
        if (q.Records.Count != 1)
            return null;

        var rec = ValidateRecord("", table, q.Records[0]);
        if (rec.Error is not null)
            return null;

        if (q.OnColumn is not null && table.HasColumn(q.OnColumn) && rec.FieldsByName.ContainsKey(q.OnColumn))
            ResolveOnColumnIds(table, q.OnColumn, [rec]);

        if ((rec.ExplicitId ?? rec.ResolvedOnId) is not { } id)
            return null;

        var place = table.Index.ReadPlace(id);
        if (place < 0 || IsExpired(table, place))
            return null;

        return GetExecutor.ReadFullRow(table, id, place);
    }

    private static bool IsExpired(TableHandle table, long place)
    {
        if (table.Ttl is not { } ttl)
            return false;
        var expiresAt = ttl.ReadExpiresAt(place);
        return expiresAt > 0 && DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() > expiresAt;
    }

    private static SproutResponse ConditionFailed(
        string query, UpsertCondition when, string reason, Dictionary<string, object?>? currentRow)
    {
        var error = new SproutError
        {
            Code = ErrorCodes.CONDITION_FAILED,
            Message = $"condition failed: {reason}",
            Position = when.Position,
            Length = when.Length,
        };
        return ResponseHelper.Errors(query, [error], currentRow is null ? [] : [currentRow]);
    }

    // ── Single-pass ON column resolution ─────────────────────────────

    private static void ResolveOnColumnIds(TableHandle table, string columnName, ParsedRecord[] parsed)
    {
        var colHandle = table.GetColumn(columnName);

        // Build lookup: encoded match value → index in parsed array
        // For null match values, track indices separately
        var nullIndices = new List<int>();
        var encodedLookup = new Dictionary<ByteKey, int>();

        for (int i = 0; i < parsed.Length; i++)
        {
            var matchField = parsed[i].FieldsByName[columnName];
            if (matchField.Value.Kind == UpsertValueKind.Null)
            {
                nullIndices.Add(i);
            }
            else
            {
                var encoded = colHandle.EncodeValueToBytes(matchField.Value.Raw!);
                encodedLookup[new ByteKey(encoded)] = i;
            }
        }

        // Indexed column: resolve values via the B-Tree (O(log n)) instead of a full
        // scan. Same result as the scan below: among several rows with the value the
        // lowest slot wins. NULLs are not in the B-Tree — those still need the scan.
        if (table.HasBTree(columnName))
        {
            var btree = table.GetBTree(columnName);
            foreach (var (key, idx) in encodedLookup)
            {
                var places = btree.Lookup(key.Bytes);
                if (places.Count == 0)
                    continue;

                var place = places.Min();
                var id = table.Index.FindIdForPlace(place);
                if (id == 0)
                    continue;

                parsed[idx].ResolvedOnId = id;
                parsed[idx].ResolvedOnPlace = place;
            }

            if (nullIndices.Count == 0)
                return;
            encodedLookup.Clear();
        }

        // Single scan over all used rows
        int remaining = encodedLookup.Count + nullIndices.Count;
        table.Index.ForEachUsed((id, place) =>
        {
            if (remaining == 0) return;

            // Check null matches
            if (nullIndices.Count > 0 && colHandle.IsNullAtPlace(place))
            {
                // Assign to first unresolved null match
                for (int n = 0; n < nullIndices.Count; n++)
                {
                    var idx = nullIndices[n];
                    if (!parsed[idx].ResolvedOnId.HasValue)
                    {
                        parsed[idx].ResolvedOnId = id;
                        parsed[idx].ResolvedOnPlace = place;
                        remaining--;
                        break;
                    }
                }
                return;
            }

            // Check encoded matches
            if (encodedLookup.Count > 0)
            {
                foreach (var (key, idx) in encodedLookup)
                {
                    if (!parsed[idx].ResolvedOnId.HasValue && colHandle.MatchesAtPlace(place, key.Bytes))
                    {
                        parsed[idx].ResolvedOnId = id;
                        parsed[idx].ResolvedOnPlace = place;
                        remaining--;
                        break;
                    }
                }
            }
        });
    }

    // ── Validation ───────────────────────────────────────────────────

    private static ParsedRecord ValidateRecord(string query, TableHandle table, List<UpsertField> fields)
    {
        ulong? explicitId = null;
        var dataFields = new List<UpsertField>(fields.Count);
        List<SproutError>? errors = null;

        foreach (var field in fields)
        {
            if (field.Name == "_id")
            {
                if (field.Value.Kind != UpsertValueKind.Integer || field.Value.Raw is null
                    || !ulong.TryParse(field.Value.Raw, out var idVal) || idVal == 0)
                {
                    errors ??= [];
                    errors.Add(new SproutError { Code = ErrorCodes.TYPE_MISMATCH, Message = "_id must be a positive integer", Position = field.Position, Length = field.Length });
                }
                else
                {
                    explicitId = idVal;
                }
                continue;
            }

            if (!table.HasColumn(field.Name))
            {
                errors ??= [];
                errors.Add(new SproutError { Code = ErrorCodes.UNKNOWN_COLUMN, Message = $"column '{field.Name}' does not exist", Position = field.Position, Length = field.Length });
                continue;
            }

            dataFields.Add(field);
        }

        // Type/null validation only for fields that exist (skip unknown columns)
        foreach (var field in dataFields)
        {
            var colHandle = table.GetColumn(field.Name);

            if (field.Value.Kind == UpsertValueKind.Null)
            {
                if (!colHandle.Schema.Nullable)
                {
                    errors ??= [];
                    errors.Add(new SproutError { Code = ErrorCodes.NOT_NULLABLE, Message = $"column '{field.Name}' is not nullable, default is '{colHandle.Schema.Default}'", Position = field.Position, Length = field.Length });
                }
                continue;
            }

            var typeError = ValidateValueType(field, colHandle.Schema);
            if (typeError is not null)
            {
                errors ??= [];
                errors.Add(new SproutError { Code = ErrorCodes.TYPE_MISMATCH, Message = typeError, Position = field.Position, Length = field.Length });
            }
        }

        if (errors is not null)
            return ParsedRecord.WithError(ResponseHelper.Errors(query, errors));

        var fieldsByName = new Dictionary<string, UpsertField>(dataFields.Count);
        foreach (var f in dataFields)
            fieldsByName[f.Name] = f;

        return new ParsedRecord
        {
            ExplicitId = explicitId,
            FieldsByName = fieldsByName,
        };
    }

    // ── Write helpers ────────────────────────────────────────────────

    private static (Dictionary<string, object?> record, ulong id, long place) WriteInsert(
        TableHandle table,
        Dictionary<string, UpsertField> fieldsByName,
        Storage.TransactionJournal? journal)
    {
        var id = table.Index.ReadNextId();
        table.Index.WriteNextId(id + 1, journal);

        var place = table.Index.FindNextPlace();
        table.Index.WritePlace(id, place, journal);

        var record = WriteRecord(table, place, id, fieldsByName, isNew: true, journal);
        return (record, id, place);
    }

    private static (Dictionary<string, object?> record, ulong id, long place) WriteWithId(
        TableHandle table,
        Dictionary<string, UpsertField> fieldsByName,
        ulong id,
        Storage.TransactionJournal? journal,
        bool fresh = false,
        long? knownPlace = null)
    {
        var existingPlace = knownPlace ?? table.Index.ReadPlace(id);
        bool isNew;
        long place;

        if (existingPlace >= 0)
        {
            isNew = false;
            place = existingPlace;
        }
        else
        {
            isNew = true;
            place = table.Index.FindNextPlace();
            table.Index.WritePlace(id, place, journal);
        }

        var currentNextId = table.Index.ReadNextId();
        if (id >= currentNextId)
            table.Index.WriteNextId(id + 1, journal);

        var record = WriteRecord(table, place, id, fieldsByName, isNew, journal, fresh);
        return (record, id, place);
    }

    /// <param name="fresh">
    /// Existing row that is rewritten as if newly inserted: columns missing from
    /// the record are reset to their default / null instead of being kept.
    /// </param>
    private static Dictionary<string, object?> WriteRecord(
        TableHandle table,
        long place,
        ulong id,
        Dictionary<string, UpsertField> fieldsByName,
        bool isNew,
        Storage.TransactionJournal? journal = null,
        bool fresh = false)
    {
        var record = new Dictionary<string, object?>(table.Schema.Columns.Count + 1);

        foreach (var colSchema in table.Schema.Columns)
        {
            var colHandle = table.GetColumn(colSchema.Name);
            var isBlob = colHandle.Type == ColumnType.Blob;
            var isArray = colHandle.Type == ColumnType.Array;
            colHandle.EnsureCapacity(place + 1);

            // Read old encoded value for B-Tree removal (only on update when B-Tree exists)
            byte[]? oldEncoded = null;
            if (!isNew && !isBlob && !isArray && table.HasBTree(colSchema.Name) && !colHandle.IsNullAtPlace(place))
            {
                var oldVal = colHandle.ReadValue(place);
                if (oldVal is not null)
                    oldEncoded = colHandle.EncodeValueToBytes(oldVal.ToString() ?? "");
            }

            if (fieldsByName.TryGetValue(colSchema.Name, out var field))
            {
                if (field.Value.Kind == UpsertValueKind.Null)
                {
                    colHandle.WriteNull(place, journal);
                    record[colSchema.Name] = null;

                    // Delete blob/array file if exists
                    if (isBlob)
                        table.DeleteBlobFile(colSchema.Name, (long)id, journal);
                    if (isArray)
                        table.DeleteArrayFile(colSchema.Name, (long)id, journal);

                    // Remove old value from B-Tree
                    if (oldEncoded is not null)
                        table.GetBTree(colSchema.Name).Remove(oldEncoded, place, journal);
                }
                else if (isBlob)
                {
                    // Decode base64 → write .blob file → store byte count in .col
                    var blobBytes = Convert.FromBase64String(field.Value.Raw!);
                    table.WriteBlobFile(colSchema.Name, (long)id, blobBytes, journal);
                    colHandle.WriteValue(place, blobBytes.Length.ToString(), journal);
                    record[colSchema.Name] = (long)blobBytes.Length;
                }
                else if (isArray)
                {
                    // Write JSON array to .array file → store element count in .col
                    var json = field.Value.Raw ?? "[]";
                    var arrayBytes = System.Text.Encoding.UTF8.GetBytes(json);
                    table.WriteArrayFile(colSchema.Name, (long)id, arrayBytes, journal);
                    var doc = System.Text.Json.JsonDocument.Parse(json);
                    var elementCount = doc.RootElement.GetArrayLength();
                    doc.Dispose();
                    colHandle.WriteValue(place, elementCount.ToString(), journal);
                    record[colSchema.Name] = System.Text.Json.JsonSerializer.Deserialize<List<object?>>(json);
                }
                else
                {
                    record[colSchema.Name] = colHandle.WriteValue(place, field.Value.Raw!, journal);

                    // Update B-Tree: remove old, insert new
                    if (table.HasBTree(colSchema.Name))
                    {
                        var btree = table.GetBTree(colSchema.Name);
                        if (oldEncoded is not null)
                            btree.Remove(oldEncoded, place, journal);
                        var newEncoded = colHandle.EncodeValueToBytes(field.Value.Raw!);
                        btree.Insert(newEncoded, place, journal);
                    }
                }
            }
            else if (isNew || fresh)
            {
                if (!isNew)
                {
                    // Rewriting an existing row: drop what the old value left behind
                    if (oldEncoded is not null)
                        table.GetBTree(colSchema.Name).Remove(oldEncoded, place, journal);
                    if (isBlob)
                        table.DeleteBlobFile(colSchema.Name, (long)id, journal);
                    if (isArray)
                        table.DeleteArrayFile(colSchema.Name, (long)id, journal);
                }

                if (colSchema.Default is not null && !isBlob && !isArray)
                {
                    record[colSchema.Name] = colHandle.WriteValue(place, colSchema.Default, journal);

                    // Insert default value into B-Tree
                    if (table.HasBTree(colSchema.Name))
                    {
                        var newEncoded = colHandle.EncodeValueToBytes(colSchema.Default);
                        table.GetBTree(colSchema.Name).Insert(newEncoded, place, journal);
                    }
                }
                else
                {
                    colHandle.WriteNull(place, journal);
                    record[colSchema.Name] = null;
                }
            }
            else
            {
                // Existing row, field not in update — read current value
                if (isBlob || isArray)
                {
                    // Return byte/element count from .col (not the actual data)
                    record[colSchema.Name] = colHandle.ReadValue(place);
                }
                else
                {
                    record[colSchema.Name] = colHandle.ReadValue(place);
                }
            }
        }

        return record;
    }

    // ── Unique constraint validation ──────────────────────────────

    private static SproutResponse? ValidateUniqueConstraints(
        string query, TableHandle table, ParsedRecord[] parsed, string? onColumn)
    {
        // Collect unique columns that have B-Trees
        var uniqueColumns = new List<string>();
        foreach (var col in table.Schema.Columns)
        {
            if (col.IsUnique && table.HasBTree(col.Name))
                uniqueColumns.Add(col.Name);
        }

        if (uniqueColumns.Count == 0) return null;

        foreach (var colName in uniqueColumns)
        {
            var colHandle = table.GetColumn(colName);
            var btree = table.GetBTree(colName);
            var seenInBatch = new Dictionary<ByteKey, int>();

            for (int i = 0; i < parsed.Length; i++)
            {
                var rec = parsed[i];
                if (!rec.FieldsByName.TryGetValue(colName, out var field)) continue;
                if (field.Value.Kind == UpsertValueKind.Null) continue; // nulls are allowed

                var encoded = colHandle.EncodeValueToBytes(field.Value.Raw!);
                var key = new ByteKey(encoded);

                // Check within batch
                if (seenInBatch.TryGetValue(key, out var prevIdx))
                    return ResponseHelper.Error(query, ErrorCodes.UNIQUE_VIOLATION,
                        $"unique constraint violation on '{colName}': duplicate value in batch (records {prevIdx + 1} and {i + 1})");

                seenInBatch[key] = i;

                // Check against existing data via B-Tree
                var existing = btree.Lookup(encoded);
                if (existing.Count > 0)
                {
                    // If updating the same row, allow it
                    var resolvedId = rec.ExplicitId ?? rec.ResolvedOnId ?? rec.ReplayId;
                    if (resolvedId.HasValue)
                    {
                        var existingPlace = rec.ExplicitId is null && rec.ResolvedOnId is not null && rec.ResolvedOnPlace is { } known
                            ? known
                            : table.Index.ReadPlace(resolvedId.Value);
                        if (existing.Count == 1 && existing[0] == existingPlace)
                            continue; // same row, no violation
                    }

                    return ResponseHelper.Error(query, ErrorCodes.UNIQUE_VIOLATION,
                        $"unique constraint violation on '{colName}': value '{field.Value.Raw}' already exists");
                }
            }
        }

        return null;
    }

    private static string? ValidateValueType(UpsertField field, ColumnSchemaEntry colSchema)
    {
        ColumnTypes.TryParse(colSchema.Type, out var colType);
        var kind = field.Value.Kind;

        return colType switch
        {
            ColumnType.Bool when kind != UpsertValueKind.Boolean =>
                $"type mismatch on '{field.Name}': expected bool, got {UpsertValueKindNames.GetName(kind)}",

            ColumnType.String when kind != UpsertValueKind.String =>
                $"type mismatch on '{field.Name}': expected string, got {UpsertValueKindNames.GetName(kind)}",

            ColumnType.SByte or ColumnType.UByte or ColumnType.SShort or ColumnType.UShort or
            ColumnType.SInt or ColumnType.UInt or ColumnType.SLong or ColumnType.ULong
                when kind is not UpsertValueKind.Integer =>
                $"type mismatch on '{field.Name}': expected {colSchema.Type}, got {UpsertValueKindNames.GetName(kind)}",

            ColumnType.Float or ColumnType.Double
                when kind is not (UpsertValueKind.Float or UpsertValueKind.Integer) =>
                $"type mismatch on '{field.Name}': expected {colSchema.Type}, got {UpsertValueKindNames.GetName(kind)}",

            ColumnType.Date or ColumnType.Time or ColumnType.DateTime
                when kind != UpsertValueKind.String =>
                $"type mismatch on '{field.Name}': expected {colSchema.Type} (as string), got {UpsertValueKindNames.GetName(kind)}",

            ColumnType.Blob when kind != UpsertValueKind.String =>
                $"type mismatch on '{field.Name}': expected blob (base64 string), got {UpsertValueKindNames.GetName(kind)}",

            ColumnType.Blob when kind == UpsertValueKind.String && !IsValidBase64(field.Value.Raw) =>
                $"type mismatch on '{field.Name}': value is not valid base64",

            ColumnType.Array when kind != UpsertValueKind.Array =>
                $"type mismatch on '{field.Name}': expected array, got {UpsertValueKindNames.GetName(kind)}",

            _ => null,
        };
    }

    private static bool IsValidBase64(string? value)
    {
        if (value is null) return false;
        Span<byte> buffer = stackalloc byte[256];
        if (value.Length > 170) // 170 chars base64 ≈ 128 bytes output
            buffer = new byte[((value.Length + 3) / 4) * 3];
        return Convert.TryFromBase64String(value, buffer, out _);
    }

    // ── Internal types ───────────────────────────────────────────────

    private sealed class ParsedRecord
    {
        public ulong? ExplicitId;
        public ulong? ResolvedOnId;
        public long? ResolvedOnPlace; // slot of ResolvedOnId, known from the 'on' scan (ReadPlace is O(n))
        public ulong? ReplayId;
        public ulong? ExpiredId; // row matched via 'on' but expired → freed before the insert
        public Dictionary<string, UpsertField> FieldsByName = null!;
        public SproutResponse? Error;

        public static ParsedRecord WithError(SproutResponse error) => new() { Error = error };
    }

}

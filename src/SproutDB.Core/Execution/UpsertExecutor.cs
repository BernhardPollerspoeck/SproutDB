using SproutDB.Core.Parsing;
using SproutDB.Core.Storage;

namespace SproutDB.Core.Execution;

internal static class UpsertExecutor
{
    public static SproutResponse Execute(string query, TableHandle table, UpsertQuery q, int bulkLimit)
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

        // ── Unique constraint validation ───────────────────────────────────
        var uniqueError = ValidateUniqueConstraints(query, table, parsed, q.OnColumn);
        if (uniqueError is not null)
            return uniqueError;

        // ── Execute all records ──────────────────────────────────────────
        var nowMs = hasTtl ? DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() : 0L;
        var tableTtlSeconds = table.Schema.TtlSeconds;

        var data = new List<Dictionary<string, object?>>(parsed.Length);
        for (int i = 0; i < parsed.Length; i++)
        {
            var rec = parsed[i];
            var resolvedId = rec.ExplicitId ?? rec.ResolvedOnId;
            Dictionary<string, object?> record;
            ulong id;
            long place;

            if (resolvedId.HasValue)
            {
                (record, id, place) = WriteWithId(table, rec.FieldsByName, resolvedId.Value);
            }
            else
            {
                (record, id, place) = WriteInsert(table, rec.FieldsByName);
            }

            // Write TTL data
            if (hasTtl)
            {
                var rowTtl = q.RowTtlSeconds[i];
                var effectiveTtl = rowTtl > 0 ? rowTtl : tableTtlSeconds;
                var expiresAt = effectiveTtl > 0 ? nowMs + effectiveTtl * 1000 : 0L;
                table.Ttl?.Write(place, expiresAt, rowTtl);
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
        Dictionary<string, UpsertField> fieldsByName)
    {
        var id = table.Index.ReadNextId();
        table.Index.WriteNextId(id + 1);

        var place = table.Index.FindNextPlace();
        table.Index.WritePlace(id, place);

        var record = WriteRecord(table, place, fieldsByName, isNew: true);
        return (record, id, place);
    }

    private static (Dictionary<string, object?> record, ulong id, long place) WriteWithId(
        TableHandle table,
        Dictionary<string, UpsertField> fieldsByName,
        ulong id)
    {
        var existingPlace = table.Index.ReadPlace(id);
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
            table.Index.WritePlace(id, place);
        }

        var currentNextId = table.Index.ReadNextId();
        if (id >= currentNextId)
            table.Index.WriteNextId(id + 1);

        var record = WriteRecord(table, place, fieldsByName, isNew);
        return (record, id, place);
    }

    private static Dictionary<string, object?> WriteRecord(
        TableHandle table,
        long place,
        Dictionary<string, UpsertField> fieldsByName,
        bool isNew)
    {
        var record = new Dictionary<string, object?>(table.Schema.Columns.Count + 1);

        foreach (var colSchema in table.Schema.Columns)
        {
            var colHandle = table.GetColumn(colSchema.Name);
            colHandle.EnsureCapacity(place + 1);

            // Read old encoded value for B-Tree removal (only on update when B-Tree exists)
            byte[]? oldEncoded = null;
            if (!isNew && table.HasBTree(colSchema.Name) && !colHandle.IsNullAtPlace(place))
            {
                var oldVal = colHandle.ReadValue(place);
                if (oldVal is not null)
                    oldEncoded = colHandle.EncodeValueToBytes(oldVal.ToString() ?? "");
            }

            if (fieldsByName.TryGetValue(colSchema.Name, out var field))
            {
                if (field.Value.Kind == UpsertValueKind.Null)
                {
                    colHandle.WriteNull(place);
                    record[colSchema.Name] = null;

                    // Remove old value from B-Tree
                    if (oldEncoded is not null)
                        table.GetBTree(colSchema.Name).Remove(oldEncoded, place);
                }
                else
                {
                    record[colSchema.Name] = colHandle.WriteValue(place, field.Value.Raw!);

                    // Update B-Tree: remove old, insert new
                    if (table.HasBTree(colSchema.Name))
                    {
                        var btree = table.GetBTree(colSchema.Name);
                        if (oldEncoded is not null)
                            btree.Remove(oldEncoded, place);
                        var newEncoded = colHandle.EncodeValueToBytes(field.Value.Raw!);
                        btree.Insert(newEncoded, place);
                    }
                }
            }
            else if (isNew)
            {
                if (colSchema.Default is not null)
                {
                    record[colSchema.Name] = colHandle.WriteValue(place, colSchema.Default);

                    // Insert default value into B-Tree
                    if (table.HasBTree(colSchema.Name))
                    {
                        var newEncoded = colHandle.EncodeValueToBytes(colSchema.Default);
                        table.GetBTree(colSchema.Name).Insert(newEncoded, place);
                    }
                }
                else
                {
                    colHandle.WriteNull(place);
                    record[colSchema.Name] = null;
                }
            }
            else
            {
                record[colSchema.Name] = colHandle.ReadValue(place);
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
                    var resolvedId = rec.ExplicitId ?? rec.ResolvedOnId;
                    if (resolvedId.HasValue)
                    {
                        var existingPlace = table.Index.ReadPlace(resolvedId.Value);
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

            _ => null,
        };
    }

    // ── Internal types ───────────────────────────────────────────────

    private sealed class ParsedRecord
    {
        public ulong? ExplicitId;
        public ulong? ResolvedOnId;
        public Dictionary<string, UpsertField> FieldsByName = null!;
        public SproutResponse? Error;

        public static ParsedRecord WithError(SproutResponse error) => new() { Error = error };
    }

}

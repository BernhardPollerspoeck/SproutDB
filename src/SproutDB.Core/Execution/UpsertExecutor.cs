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
                        "cannot combine explicit id with 'on' clause");

                if (!parsed[i].FieldsByName.ContainsKey(q.OnColumn))
                    return ResponseHelper.Error(query, ErrorCodes.SYNTAX_ERROR,
                        $"on column '{q.OnColumn}' must be included in the upsert fields");
            }

            // Single-pass scan: collect all match values, scan once, resolve IDs
            ResolveOnColumnIds(table, q.OnColumn, parsed);
        }

        // ── Execute all records ──────────────────────────────────────────
        var data = new List<Dictionary<string, object?>>(parsed.Length);
        foreach (var rec in parsed)
        {
            var resolvedId = rec.ExplicitId ?? rec.ResolvedOnId;
            Dictionary<string, object?> record;
            ulong id;

            if (resolvedId.HasValue)
            {
                (record, id) = WriteWithId(table, rec.FieldsByName, resolvedId.Value);
            }
            else
            {
                (record, id) = WriteInsert(table, rec.FieldsByName);
            }

            record["id"] = id;
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
            if (field.Name == "id")
            {
                if (field.Value.Kind != UpsertValueKind.Integer || field.Value.Raw is null
                    || !ulong.TryParse(field.Value.Raw, out var idVal) || idVal == 0)
                {
                    errors ??= [];
                    errors.Add(new SproutError { Code = ErrorCodes.TYPE_MISMATCH, Message = "id must be a positive integer", Position = field.Position, Length = field.Length });
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

    private static (Dictionary<string, object?> record, ulong id) WriteInsert(
        TableHandle table,
        Dictionary<string, UpsertField> fieldsByName)
    {
        var id = table.Index.ReadNextId();
        table.Index.WriteNextId(id + 1);

        var place = table.Index.FindNextPlace();
        table.Index.WritePlace(id, place);

        var record = WriteRecord(table, place, fieldsByName, isNew: true);
        return (record, id);
    }

    private static (Dictionary<string, object?> record, ulong id) WriteWithId(
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
        return (record, id);
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

    /// <summary>
    /// Wrapper for byte[] that implements value equality for use as dictionary key.
    /// </summary>
    private readonly struct ByteKey : IEquatable<ByteKey>
    {
        public readonly byte[] Bytes;

        public ByteKey(byte[] bytes) => Bytes = bytes;

        public bool Equals(ByteKey other) => Bytes.AsSpan().SequenceEqual(other.Bytes);
        public override bool Equals(object? obj) => obj is ByteKey other && Equals(other);

        public override int GetHashCode()
        {
            var hash = new HashCode();
            foreach (var b in Bytes)
                hash.Add(b);
            return hash.ToHashCode();
        }
    }
}

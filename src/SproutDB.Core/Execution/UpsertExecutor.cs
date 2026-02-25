using SproutDB.Core.Parsing;
using SproutDB.Core.Storage;

namespace SproutDB.Core.Execution;

internal static class UpsertExecutor
{
    public static SproutResponse Execute(string query, TableHandle table, UpsertQuery q)
    {
        // Separate id field from data fields, validate
        ulong? explicitId = null;
        var dataFields = new List<UpsertField>(q.Fields.Count);

        foreach (var field in q.Fields)
        {
            if (field.Name == "id")
            {
                if (field.Value.Kind != UpsertValueKind.Integer || field.Value.Raw is null)
                    return ResponseHelper.Error(query, ErrorCodes.TYPE_MISMATCH, "id must be a positive integer");

                if (!ulong.TryParse(field.Value.Raw, out var idVal) || idVal == 0)
                    return ResponseHelper.Error(query, ErrorCodes.TYPE_MISMATCH, "id must be a positive integer");

                explicitId = idVal;
                continue;
            }

            if (!table.HasColumn(field.Name))
                return ResponseHelper.Error(query, ErrorCodes.UNKNOWN_COLUMN,
                    $"column '{field.Name}' does not exist");

            dataFields.Add(field);
        }

        // Validate types and nullable constraints
        foreach (var field in dataFields)
        {
            var colHandle = table.GetColumn(field.Name);

            if (field.Value.Kind == UpsertValueKind.Null)
            {
                if (!colHandle.Schema.Nullable)
                    return ResponseHelper.Error(query, ErrorCodes.NOT_NULLABLE,
                        $"column '{field.Name}' is not nullable, default is '{colHandle.Schema.Default}'");
                continue;
            }

            var typeError = ValidateValueType(field, colHandle.Schema);
            if (typeError is not null)
                return ResponseHelper.Error(query, ErrorCodes.TYPE_MISMATCH, typeError);
        }

        // Build field lookup
        var fieldsByName = new Dictionary<string, UpsertField>(dataFields.Count);
        foreach (var f in dataFields)
            fieldsByName[f.Name] = f;

        if (explicitId.HasValue)
            return ExecuteWithId(table, fieldsByName, explicitId.Value);

        return ExecuteInsert(table, fieldsByName);
    }

    private static SproutResponse ExecuteInsert(
        TableHandle table,
        Dictionary<string, UpsertField> fieldsByName)
    {
        var id = table.Index.ReadNextId();
        table.Index.WriteNextId(id + 1);

        var place = table.Index.FindNextPlace();
        table.Index.WritePlace(id, place);

        var record = WriteRecord(table, place, fieldsByName, isNew: true);
        record["id"] = id;

        return new SproutResponse
        {
            Operation = SproutOperation.Upsert,
            Data = [record],
            Affected = 1,
        };
    }

    private static SproutResponse ExecuteWithId(
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
        record["id"] = id;

        return new SproutResponse
        {
            Operation = SproutOperation.Upsert,
            Data = [record],
            Affected = 1,
        };
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

            if (fieldsByName.TryGetValue(colSchema.Name, out var field))
            {
                if (field.Value.Kind == UpsertValueKind.Null)
                {
                    colHandle.WriteNull(place);
                    record[colSchema.Name] = null;
                }
                else
                {
                    record[colSchema.Name] = colHandle.WriteValue(place, field.Value.Raw!);
                }
            }
            else if (isNew)
            {
                if (colSchema.Default is not null)
                {
                    record[colSchema.Name] = colHandle.WriteValue(place, colSchema.Default);
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
}

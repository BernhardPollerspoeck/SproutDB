using SproutDB.Core.Parsing;
using SproutDB.Core.Storage;

namespace SproutDB.Core.Execution;

internal static class ResponseHelper
{
    public static SproutResponse Error(string query, string code, string message)
    {
        return new SproutResponse
        {
            Operation = SproutOperation.Error,
            Errors = [new SproutError { Code = code, Message = message }],
            AnnotatedQuery = $"{query} ##{message}##",
        };
    }

    public static SproutResponse Errors(string query, List<SproutError> errors)
    {
        return new SproutResponse
        {
            Operation = SproutOperation.Error,
            Errors = errors,
            AnnotatedQuery = BuildAnnotatedQuery(query, errors),
        };
    }

    private static string BuildAnnotatedQuery(string query, List<SproutError> errors)
    {
        // Sort errors by position (inline first, then suffix for position=-1)
        // Stable sort preserves original order for errors at the same position
        var sorted = new List<SproutError>(errors);
        sorted.Sort((a, b) =>
        {
            // Errors without position go to the end (suffix)
            if (a.Position < 0 && b.Position < 0) return 0;
            if (a.Position < 0) return 1;
            if (b.Position < 0) return -1;
            return a.Position.CompareTo(b.Position);
        });

        var sb = new System.Text.StringBuilder(query.Length + errors.Count * 40);
        var lastPos = 0;

        foreach (var error in sorted)
        {
            if (error.Position < 0)
            {
                // No position — append as suffix
                if (lastPos < query.Length)
                {
                    sb.Append(query, lastPos, query.Length - lastPos);
                    lastPos = query.Length;
                }
                sb.Append(" ##");
                sb.Append(error.Message);
                sb.Append("##");
                continue;
            }

            var errorEnd = error.Position + error.Length;

            // Copy query text up to end of error token
            if (errorEnd > lastPos)
            {
                sb.Append(query, lastPos, errorEnd - lastPos);
                lastPos = errorEnd;
            }

            sb.Append(" ##");
            sb.Append(error.Message);
            sb.Append("##");
        }

        // Remaining query text after last inline annotation
        if (lastPos < query.Length)
            sb.Append(query, lastPos, query.Length - lastPos);

        return sb.ToString();
    }

    public static SproutResponse ParseError(ParseResult result)
    {
        return new SproutResponse
        {
            Operation = SproutOperation.Error,
            Errors = result.Errors!
                .Select(e => new SproutError { Code = e.Code, Message = e.Message })
                .ToList(),
            AnnotatedQuery = result.AnnotatedQuery,
        };
    }

    public static List<ColumnInfo> BuildColumnInfoList(List<ColumnDefinition> columns)
    {
        var result = new List<ColumnInfo>(columns.Count + 1) { IdColumnInfo() };

        foreach (var col in columns)
        {
            result.Add(new ColumnInfo
            {
                Name = col.Name,
                Type = ColumnTypes.GetName(col.Type),
                Size = col.Type == ColumnType.String ? col.Size : null,
                Nullable = col.IsNullable,
                Default = col.Default,
                Strict = col.Strict,
            });
        }

        return result;
    }

    public static List<ColumnInfo> BuildColumnInfoList(
        TableSchema schema,
        Func<string, bool>? isIndexed = null,
        Func<string, bool>? isAutoIndex = null)
    {
        var result = new List<ColumnInfo>(schema.Columns.Count + 1) { IdColumnInfo() };

        foreach (var col in schema.Columns)
        {
            ColumnTypes.TryParse(col.Type, out var colType);
            var indexed = isIndexed?.Invoke(col.Name) ?? false;
            result.Add(new ColumnInfo
            {
                Name = col.Name,
                Type = col.Type,
                Size = colType == ColumnType.String ? col.Size : null,
                Nullable = col.Nullable,
                Default = col.Default,
                Strict = col.Strict,
                Indexed = indexed,
                IsAutoIndex = indexed && (isAutoIndex?.Invoke(col.Name) ?? false),
                IsUnique = col.IsUnique,
            });
        }

        return result;
    }

    private static ColumnInfo IdColumnInfo() => new()
    {
        Name = "_id",
        Type = "ulong",
        Nullable = false,
        Default = null,
        Strict = true,
        Auto = true,
    };
}

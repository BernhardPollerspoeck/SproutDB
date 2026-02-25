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

    public static List<ColumnInfo> BuildColumnInfoList(TableSchema schema)
    {
        var result = new List<ColumnInfo>(schema.Columns.Count + 1) { IdColumnInfo() };

        foreach (var col in schema.Columns)
        {
            ColumnTypes.TryParse(col.Type, out var colType);
            result.Add(new ColumnInfo
            {
                Name = col.Name,
                Type = col.Type,
                Size = colType == ColumnType.String ? col.Size : null,
                Nullable = col.Nullable,
                Default = col.Default,
                Strict = col.Strict,
            });
        }

        return result;
    }

    private static ColumnInfo IdColumnInfo() => new()
    {
        Name = "id",
        Type = "ulong",
        Nullable = false,
        Default = null,
        Strict = true,
        Auto = true,
    };
}

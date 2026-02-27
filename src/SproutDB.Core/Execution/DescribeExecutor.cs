using SproutDB.Core.Storage;

namespace SproutDB.Core.Execution;

internal static class DescribeExecutor
{
    /// <summary>
    /// DESCRIBE TABLE — returns all columns with type info.
    /// </summary>
    public static SproutResponse ExecuteTable(string query, TableHandle table, string tableName)
    {
        var columns = ResponseHelper.BuildColumnInfoList(table.Schema);

        return new SproutResponse
        {
            Operation = SproutOperation.Describe,
            Schema = new SchemaInfo
            {
                Table = tableName,
                Columns = columns,
            },
        };
    }

    /// <summary>
    /// DESCRIBE ALL — returns all table names in the database.
    /// </summary>
    public static SproutResponse ExecuteAll(string query, string dbPath)
    {
        var tables = new List<string>();

        if (Directory.Exists(dbPath))
        {
            foreach (var dir in Directory.GetDirectories(dbPath))
            {
                var schemaPath = Path.Combine(dir, "_schema.bin");
                if (File.Exists(schemaPath))
                    tables.Add(Path.GetFileName(dir));
            }

            tables.Sort(StringComparer.Ordinal);
        }

        return new SproutResponse
        {
            Operation = SproutOperation.Describe,
            Schema = new SchemaInfo
            {
                Tables = tables,
            },
        };
    }
}

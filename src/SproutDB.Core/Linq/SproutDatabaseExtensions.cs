using SproutDB.Core.Linq;

namespace SproutDB.Core;

public static class SproutDatabaseExtensions
{
    public static SproutTable<T> Table<T>(this ISproutDatabase db, string tableName)
        where T : class, ISproutEntity, new()
    {
        return new SproutTable<T>(db, tableName);
    }

    /// <summary>
    /// Returns a fluent builder for creating a table.
    /// </summary>
    public static CreateTableBuilder CreateTable(this ISproutDatabase db, string tableName)
    {
        return new CreateTableBuilder(db, tableName);
    }

    /// <summary>
    /// Adds a column to an existing table.
    /// Generates: add column {table}.{column} {type} [size] [default {value}]
    /// </summary>
    public static SproutResponse AddColumn<T>(this ISproutDatabase db, string table, string column,
        int size = 0, string? defaultValue = null)
    {
        var typeName = FluentTypeMapper.GetTypeName(typeof(T));

        if (FluentTypeMapper.RequiresSize(typeName) && size <= 0)
            throw new ArgumentException($"String column '{column}' requires a size > 0.");

        var query = $"add column {table}.{column} {typeName}";
        if (size > 0)
            query += $" {size}";
        if (defaultValue is not null)
            query += $" default {defaultValue}";

        var result = db.Query(query)[0];

        if (result.Errors is not null && result.Errors.Count > 0)
            throw new SproutQueryException(result.Errors[0].Message);

        return result;
    }

    /// <summary>
    /// Alters a column's size in an existing table.
    /// Generates: alter column {table}.{column} {size}
    /// </summary>
    public static SproutResponse AlterColumn(this ISproutDatabase db, string table, string column, int size)
    {
        var query = $"alter column {table}.{column} string {size}";
        var result = db.Query(query)[0];

        if (result.Errors is not null && result.Errors.Count > 0)
            throw new SproutQueryException(result.Errors[0].Message);

        return result;
    }
}

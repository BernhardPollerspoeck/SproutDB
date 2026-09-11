using SproutDB.Core.Execution;
using SproutDB.Core.Linq;
using SproutDB.Core.Parsing;

namespace SproutDB.Core;

public static class SproutDatabaseExtensions
{
    /// <summary>
    /// Runs a query with <c>@name</c> placeholders, bound from the public properties
    /// of <paramref name="parameters"/> (e.g. <c>new { key = grainKey }</c>) or from a
    /// dictionary. Values are inserted as literals only — never as query syntax — so
    /// no escaping is needed. Placeholders are allowed where values go (WHERE values,
    /// upsert values, <c>in [...]</c> / <c>in @list</c>, <c>when</c>), not for names.
    /// Missing or unused parameters fail with <c>PARAMETER_ERROR</c>.
    /// </summary>
    public static List<SproutResponse> Query(this ISproutDatabase db, string query, object parameters)
        => db.Query(query, QueryParameters.FromObject(parameters));

    /// <inheritdoc cref="Query(ISproutDatabase, string, object)"/>
    public static List<SproutResponse> Query(this ISproutDatabase db, string query,
        IReadOnlyDictionary<string, object?> parameters)
    {
        if (!QueryParameters.TryBind(query, parameters, out var bound, out var error))
            return [error ?? ResponseHelper.Error(query, ErrorCodes.PARAMETER_ERROR, "invalid parameters")];

        return db.Query(bound);
    }

    /// <summary>
    /// Asynchronous <see cref="Query(ISproutDatabase, string, object)"/> — see
    /// <see cref="ISproutDatabase.QueryAsync"/> for the cancellation semantics.
    /// </summary>
    public static ValueTask<List<SproutResponse>> QueryAsync(this ISproutDatabase db, string query, object parameters,
        CancellationToken cancellationToken = default)
        => db.QueryAsync(query, QueryParameters.FromObject(parameters), cancellationToken);

    /// <inheritdoc cref="QueryAsync(ISproutDatabase, string, object, CancellationToken)"/>
    public static ValueTask<List<SproutResponse>> QueryAsync(this ISproutDatabase db, string query,
        IReadOnlyDictionary<string, object?> parameters, CancellationToken cancellationToken = default)
    {
        if (!QueryParameters.TryBind(query, parameters, out var bound, out var error))
            return new ValueTask<List<SproutResponse>>(
                [error ?? ResponseHelper.Error(query, ErrorCodes.PARAMETER_ERROR, "invalid parameters")]);

        return db.QueryAsync(bound, cancellationToken);
    }

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

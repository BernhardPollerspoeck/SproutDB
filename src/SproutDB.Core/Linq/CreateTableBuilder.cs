using System.Text;

namespace SproutDB.Core.Linq;

/// <summary>
/// Fluent builder for CREATE TABLE queries.
/// Generates a query string and executes it via <see cref="ISproutDatabase.Query"/>.
/// </summary>
public sealed class CreateTableBuilder
{
    private readonly ISproutDatabase _db;
    private readonly string _tableName;
    private readonly List<ColumnDef> _columns = [];

    internal CreateTableBuilder(ISproutDatabase db, string tableName)
    {
        _db = db;
        _tableName = tableName;
    }

    /// <summary>
    /// Adds a column definition to the table.
    /// </summary>
    /// <typeparam name="T">The CLR type to map to a SproutDB type.</typeparam>
    /// <param name="name">Column name.</param>
    /// <param name="size">Size for string columns (required for strings).</param>
    /// <param name="strict">Whether the column is strict.</param>
    /// <param name="defaultValue">Default value expression (e.g. "true", "'hello'").</param>
    public CreateTableBuilder AddColumn<T>(string name, int size = 0, bool strict = false, string? defaultValue = null)
    {
        var typeName = FluentTypeMapper.GetTypeName(typeof(T));

        if (FluentTypeMapper.RequiresSize(typeName) && size <= 0)
            throw new ArgumentException($"String column '{name}' requires a size > 0.");

        _columns.Add(new ColumnDef(name, typeName, size, strict, defaultValue));
        return this;
    }

    /// <summary>
    /// Builds the query string and executes it.
    /// </summary>
    public SproutResponse Execute()
    {
        var query = BuildQuery();
        var result = _db.Query(query);

        if (result.Errors is not null && result.Errors.Count > 0)
            throw new SproutQueryException(result.Errors[0].Message);

        return result;
    }

    internal string BuildQuery()
    {
        var sb = new StringBuilder();
        sb.Append("create table ");
        sb.Append(_tableName);

        if (_columns.Count > 0)
        {
            sb.Append(" (");
            for (int i = 0; i < _columns.Count; i++)
            {
                if (i > 0) sb.Append(", ");
                var col = _columns[i];
                sb.Append(col.Name);
                sb.Append(' ');
                sb.Append(col.TypeName);

                if (col.Size > 0)
                {
                    sb.Append(' ');
                    sb.Append(col.Size);
                }

                if (col.Strict)
                    sb.Append(" strict");

                if (col.Default is not null)
                {
                    sb.Append(" default ");
                    sb.Append(col.Default);
                }
            }
            sb.Append(')');
        }

        return sb.ToString();
    }

    private sealed record ColumnDef(string Name, string TypeName, int Size, bool Strict, string? Default);
}

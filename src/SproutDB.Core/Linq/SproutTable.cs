using System.Linq.Expressions;
using System.Text;
using SproutDB.Core.Linq;

namespace SproutDB.Core;

public sealed class SproutTable<T> where T : class, ISproutEntity, new()
{
    private readonly ISproutDatabase _db;
    private readonly string _tableName;

    private string? _whereClause;
    private List<string>? _selectColumns;
    private string? _orderByColumn;
    private bool _orderByDescending;
    private int? _limit;
    private bool _isCount;
    private bool _isDistinct;

    internal SproutTable(ISproutDatabase db, string tableName)
    {
        _db = db;
        _tableName = tableName;
    }

    // ── Fluent builder ──────────────────────────────────────────

    public SproutTable<T> Where(Expression<Func<T, bool>> predicate)
    {
        _whereClause = SproutExpressionVisitor.ConvertWhere(predicate);
        return this;
    }

    public SproutTable<T> Select(Expression<Func<T, object>> selector)
    {
        _selectColumns = SproutExpressionVisitor.ConvertSelect<T, object>(selector);
        return this;
    }

    public SproutTable<T> OrderBy<TKey>(Expression<Func<T, TKey>> keySelector)
    {
        _orderByColumn = SproutExpressionVisitor.ConvertOrderBy(keySelector);
        _orderByDescending = false;
        return this;
    }

    public SproutTable<T> OrderByDescending<TKey>(Expression<Func<T, TKey>> keySelector)
    {
        _orderByColumn = SproutExpressionVisitor.ConvertOrderBy(keySelector);
        _orderByDescending = true;
        return this;
    }

    public SproutTable<T> Take(int count)
    {
        _limit = count;
        return this;
    }

    public SproutTable<T> Distinct()
    {
        _isDistinct = true;
        return this;
    }

    // ── Terminal operations (read) ──────────────────────────────

    public SproutResponse Run()
    {
        var query = BuildGetQuery();
        return _db.Query(query)[0];
    }

    public List<T> ToList()
    {
        var response = Run();
        if (response.Errors is not null && response.Errors.Count > 0)
            throw new SproutQueryException(response.Errors[0].Message);

        if (response.Data is null)
            return [];

        var result = new List<T>(response.Data.Count);
        foreach (var row in response.Data)
            result.Add(TypeMapper.Deserialize<T>(row));
        return result;
    }

    public T? FirstOrDefault(Expression<Func<T, bool>>? predicate = null)
    {
        if (predicate is not null)
            _whereClause = SproutExpressionVisitor.ConvertWhere(predicate);

        _limit = 1;
        var response = Run();

        if (response.Errors is not null && response.Errors.Count > 0)
            throw new SproutQueryException(response.Errors[0].Message);

        if (response.Data is null || response.Data.Count == 0)
            return null;

        return TypeMapper.Deserialize<T>(response.Data[0]);
    }

    public int Count()
    {
        _isCount = true;
        var response = Run();

        if (response.Errors is not null && response.Errors.Count > 0)
            throw new SproutQueryException(response.Errors[0].Message);

        return response.Affected;
    }

    // ── Upsert operations ───────────────────────────────────────

    public SproutResponse Upsert(T record)
    {
        var fields = TypeMapper.SerializeToUpsertFields(record);
        return _db.Query($"upsert {_tableName} {fields}")[0];
    }

    public SproutResponse Upsert(object record)
    {
        var fields = TypeMapper.SerializeToUpsertFields(record);
        return _db.Query($"upsert {_tableName} {fields}")[0];
    }

    public SproutResponse Upsert(T record, Expression<Func<T, object>> on)
    {
        var fields = TypeMapper.SerializeToUpsertFields(record);
        var onColumn = SproutExpressionVisitor.ConvertMemberName<T, object>(on);
        return _db.Query($"upsert {_tableName} {fields} on {onColumn}")[0];
    }

    public SproutResponse Upsert(IEnumerable<T> records, Expression<Func<T, object>> on)
    {
        var sb = new StringBuilder();
        sb.Append($"upsert {_tableName} [");
        var first = true;
        foreach (var record in records)
        {
            if (!first) sb.Append(", ");
            first = false;
            sb.Append(TypeMapper.SerializeToUpsertFields(record));
        }
        sb.Append(']');
        sb.Append($" on {SproutExpressionVisitor.ConvertMemberName<T, object>(on)}");
        return _db.Query(sb.ToString())[0];
    }

    public SproutResponse Upsert(IEnumerable<T> records)
    {
        var sb = new StringBuilder();
        sb.Append($"upsert {_tableName} [");
        var first = true;
        foreach (var record in records)
        {
            if (!first) sb.Append(", ");
            first = false;
            sb.Append(TypeMapper.SerializeToUpsertFields(record));
        }
        sb.Append(']');
        return _db.Query(sb.ToString())[0];
    }

    // ── Delete ──────────────────────────────────────────────────

    public SproutResponse Delete(Expression<Func<T, bool>> predicate)
    {
        var whereClause = SproutExpressionVisitor.ConvertWhere(predicate);
        return _db.Query($"delete {_tableName} where {whereClause}")[0];
    }

    // ── Query string builder ────────────────────────────────────

    private string BuildGetQuery()
    {
        var sb = new StringBuilder();
        sb.Append("get ");
        sb.Append(_tableName);

        if (_selectColumns is not null && _selectColumns.Count > 0)
        {
            sb.Append(" select ");
            sb.Append(string.Join(", ", _selectColumns));
        }

        if (_isDistinct)
            sb.Append(" distinct");

        if (_whereClause is not null)
        {
            sb.Append(" where ");
            sb.Append(_whereClause);
        }

        if (_isCount)
            sb.Append(" count");

        if (_orderByColumn is not null)
        {
            sb.Append(" order by ");
            sb.Append(_orderByColumn);
            if (_orderByDescending)
                sb.Append(" desc");
        }

        if (_limit.HasValue)
        {
            sb.Append(" limit ");
            sb.Append(_limit.Value);
        }

        return sb.ToString();
    }
}

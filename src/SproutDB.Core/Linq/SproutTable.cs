using System.Linq.Expressions;
using System.Text;
using SproutDB.Core.Linq;

namespace SproutDB.Core;

public sealed class SproutTable<T> where T : class, ISproutEntity, new()
{
    private readonly ISproutDatabase _db;
    private readonly string _tableName;

    private string? _whereClause;
    private ParameterCollector _whereParameters = new();
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
        SetWhere(predicate);
        return this;
    }

    private void SetWhere(Expression<Func<T, bool>> predicate)
    {
        // Fresh collector: a replaced where clause must not leave unused parameters behind
        _whereParameters = new ParameterCollector();
        _whereClause = SproutExpressionVisitor.ConvertWhere(predicate, _whereParameters);
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
        return _db.Query(query, _whereParameters.Values)[0];
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
            SetWhere(predicate);

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

    // Values always travel as parameters (@p0, @p1, …) — never as hand-built literals.

    public SproutResponse Upsert(T record)
    {
        var parameters = new ParameterCollector();
        var fields = TypeMapper.SerializeToUpsertFields(record, parameters);
        return _db.Query($"upsert {_tableName} {fields}", parameters.Values)[0];
    }

    public SproutResponse Upsert(object record)
    {
        var parameters = new ParameterCollector();
        var fields = TypeMapper.SerializeToUpsertFields(record, parameters);
        return _db.Query($"upsert {_tableName} {fields}", parameters.Values)[0];
    }

    public SproutResponse Upsert(T record, Expression<Func<T, object>> on)
    {
        var parameters = new ParameterCollector();
        var fields = TypeMapper.SerializeToUpsertFields(record, parameters);
        var onColumn = SproutExpressionVisitor.ConvertMemberName<T, object>(on);
        return _db.Query($"upsert {_tableName} {fields} on {onColumn}", parameters.Values)[0];
    }

    /// <summary>
    /// Conditional upsert: writes only if <paramref name="when"/> holds for the
    /// stored row found via <paramref name="on"/>. Otherwise the response carries
    /// <c>CONDITION_FAILED</c> and the current row in <c>Data</c> (empty if missing).
    /// </summary>
    public SproutResponse Upsert(T record, Expression<Func<T, object>> on, Expression<Func<T, bool>> when)
    {
        var parameters = new ParameterCollector();
        var fields = TypeMapper.SerializeToUpsertFields(record, parameters);
        var onColumn = SproutExpressionVisitor.ConvertMemberName<T, object>(on);
        var condition = SproutExpressionVisitor.ConvertWhere(when, parameters);
        return _db.Query($"upsert {_tableName} {fields} on {onColumn} when {condition}", parameters.Values)[0];
    }

    /// <summary>
    /// With <paramref name="ifNotExists"/> = true: inserts only if no row matches
    /// <paramref name="on"/>, otherwise <c>CONDITION_FAILED</c> with the existing row.
    /// </summary>
    public SproutResponse Upsert(T record, Expression<Func<T, object>> on, bool ifNotExists)
    {
        if (!ifNotExists)
            return Upsert(record, on);

        var parameters = new ParameterCollector();
        var fields = TypeMapper.SerializeToUpsertFields(record, parameters);
        var onColumn = SproutExpressionVisitor.ConvertMemberName<T, object>(on);
        return _db.Query($"upsert {_tableName} {fields} on {onColumn} when not exists", parameters.Values)[0];
    }

    public SproutResponse Upsert(IEnumerable<T> records, Expression<Func<T, object>> on)
    {
        var parameters = new ParameterCollector();
        var query = BuildBulkUpsert(records, parameters)
            + $" on {SproutExpressionVisitor.ConvertMemberName<T, object>(on)}";
        return _db.Query(query, parameters.Values)[0];
    }

    public SproutResponse Upsert(IEnumerable<T> records)
    {
        var parameters = new ParameterCollector();
        return _db.Query(BuildBulkUpsert(records, parameters), parameters.Values)[0];
    }

    private string BuildBulkUpsert(IEnumerable<T> records, ParameterCollector parameters)
    {
        var sb = new StringBuilder();
        sb.Append($"upsert {_tableName} [");
        var first = true;
        foreach (var record in records)
        {
            if (!first) sb.Append(", ");
            first = false;
            sb.Append(TypeMapper.SerializeToUpsertFields(record, parameters));
        }
        sb.Append(']');
        return sb.ToString();
    }

    // ── Delete ──────────────────────────────────────────────────

    public SproutResponse Delete(Expression<Func<T, bool>> predicate)
    {
        var parameters = new ParameterCollector();
        var whereClause = SproutExpressionVisitor.ConvertWhere(predicate, parameters);
        return _db.Query($"delete {_tableName} where {whereClause}", parameters.Values)[0];
    }

    /// <summary>
    /// Deletes only if exactly <paramref name="expect"/> rows match — otherwise nothing
    /// is deleted and the response carries <c>EXPECTATION_FAILED</c>.
    /// </summary>
    public SproutResponse Delete(Expression<Func<T, bool>> predicate, int expect)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(expect);
        var parameters = new ParameterCollector();
        var whereClause = SproutExpressionVisitor.ConvertWhere(predicate, parameters);
        return _db.Query($"delete {_tableName} where {whereClause} expect {expect}", parameters.Values)[0];
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

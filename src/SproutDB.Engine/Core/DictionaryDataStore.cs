using SproutDB.Engine.Compilation;
using SproutDB.Engine.Execution;

namespace SproutDB.Engine.Core;

public class DictionaryDataStore(ISproutDB server) : IDataStore
{
    private int _nextRowId = 1;
    private static readonly string[] _first = ["ID"];

    #region Table Operations

    public IEnumerable<Row> GetRows(string tableName, string? tableAlias, int? limit = null)
    {
        var database = server.GetCurrentDatabase() ?? throw new InvalidOperationException("No current database selected");
        if (!database.Tables.TryGetValue(tableName, out var table))
        {
            return [];
        }

        var rows = table.Rows.Values.AsEnumerable();

        if (tableAlias != null)
        {
            rows = rows.Select(r =>
            {
                r.Fields = r.Fields.ToDictionary(k => $"{tableAlias}.{k.Key}", v => v.Value);
                return r;
            });
        }

        // Apply limit if provided - this is for now a server ram guard
        if (limit.HasValue)
        {
            rows = rows.Take(limit.Value);
        }

        return rows;
    }

    public Row? GetRow(string tableName, object id)
    {
        var database = server.GetCurrentDatabase() ?? throw new InvalidOperationException("No current database selected");
        return !database.Tables.TryGetValue(tableName, out var table)
            ? null
            : table.Rows.TryGetValue(id, out var row) ? row : null;
    }

    public string UpsertRow(string tableName, Row row, string? onField = null)
    {
        var database = server.GetCurrentDatabase() ?? throw new InvalidOperationException("No current database selected");
        if (!database.Tables.TryGetValue(tableName, out var table))
        {
            throw new InvalidOperationException($"Table '{tableName}' does not exist");
        }

        // Determine the key for upsert
        object key;

        if (onField != null)
        {
            // Use specified field as key
            if (!row.Fields.TryGetValue(onField, out var fieldValue) || fieldValue == null)
            {
                throw new InvalidOperationException($"Field '{onField}' not found or is null in upsert data");
            }
            key = fieldValue;

            // Find existing row with this field value
            var existingRow = table.Rows.Values.FirstOrDefault(r =>
                r.Fields.TryGetValue(onField, out var existingValue) &&
                Equals(existingValue, fieldValue));

            if (existingRow != null)
            {
                key = existingRow.Id;
            }
        }
        else if (row.Id != null && !Equals(row.Id, default))
        {
            // Use provided ID
            key = row.Id;
        }
        else
        {
            // Generate new ID
            key = _nextRowId++;
        }

        // Set the ID on the row
        row.Id = key;

        // Upsert the row
        table.Rows[key] = row;

        return key.ToString()!;
    }

    public int DeleteRows(string tableName, Expression? filter = null)
    {
        var database = server.GetCurrentDatabase() ?? throw new InvalidOperationException("No current database selected");
        if (!database.Tables.TryGetValue(tableName, out var table))
        {
            return 0;
        }

        if (filter == null)
        {
            // Delete all rows
            var count = table.Rows.Count;
            table.Rows.Clear();
            return count;
        }

        // Delete rows matching filter
        var rowsToDelete = table.Rows.Values
            .Where(row => EvaluateFilter(row, filter.Value))
            .ToList();

        foreach (var row in rowsToDelete)
        {
            table.Rows.Remove(row.Id);
        }

        return rowsToDelete.Count;
    }

    public int CountRows(string tableName, Expression? filter = null)
    {
        var database = server.GetCurrentDatabase() ?? throw new InvalidOperationException("No current database selected");
        return database.Tables.TryGetValue(tableName, out var table)
            ? filter != null
                ? table.Rows.Values.Count(row => EvaluateFilter(row, filter.Value))
                : table.Rows.Count
            : 0;
    }

    #endregion

    #region Schema Operations

    public void CreateTable(string tableName)
    {
        var database = server.GetCurrentDatabase() ?? throw new InvalidOperationException("No current database selected");
        if (database.Tables.ContainsKey(tableName))
        {
            throw new InvalidOperationException($"Table '{tableName}' already exists");
        }

        database.Tables[tableName] = new Table();
    }

    public void DropTable(string tableName)
    {
        var database = server.GetCurrentDatabase() ?? throw new InvalidOperationException("No current database selected");
        if (!database.Tables.ContainsKey(tableName))
        {
            throw new InvalidOperationException($"Table '{tableName}' does not exist");
        }

        database.Tables.Remove(tableName);
    }

    public void AddColumn(string tableName, string columnName, EColumnType type)
    {
        var database = server.GetCurrentDatabase() ?? throw new InvalidOperationException("No current database selected");
        if (!database.Tables.TryGetValue(tableName, out var table))
        {
            throw new InvalidOperationException($"Table '{tableName}' does not exist");
        }

        // Add column to the schema if not exists
        if (!table.Columns.ContainsKey(columnName))
        {
            table.Columns[columnName] = new Column(type);
        }
    }

    public void PurgeColumn(string tableName, string columnName)
    {
        var database = server.GetCurrentDatabase() ?? throw new InvalidOperationException("No current database selected");
        if (!database.Tables.TryGetValue(tableName, out var table))
        {
            throw new InvalidOperationException($"Table '{tableName}' does not exist");
        }

        // Remove the column from schema
        table.Columns.Remove(columnName);

        // Remove the field from all rows
        foreach (var row in table.Rows.Values)
        {
            row.Fields.Remove(columnName);
        }
    }

    #endregion

    #region Helper Methods

    private bool EvaluateFilter(Row row, Expression filter)
    {
        // Simple filter evaluation - you can expand this
        return filter.Type switch
        {
            ExpressionType.Comparison => EvaluateComparison(row, filter),
            ExpressionType.Binary => EvaluateBinary(row, filter),
            ExpressionType.Unary => EvaluateUnary(row, filter),
            _ => true // Default to include row if we can't evaluate
        };
    }

    private static bool EvaluateComparison(Row row, Expression comparison)
    {
        var compData = comparison.As<Expression.ComparisonData>();

        // Get left value (field name)
        var leftValue = GetFieldValue(row, compData.Left);

        // Get right value (literal)
        var rightValue = GetLiteralValue(compData.Right);

        return compData.Operator switch
        {
            ComparisonOperator.Equals => Equals(leftValue, rightValue),
            ComparisonOperator.NotEquals => !Equals(leftValue, rightValue),
            ComparisonOperator.GreaterThan => CompareValues(leftValue, rightValue) > 0,
            ComparisonOperator.GreaterThanOrEqual => CompareValues(leftValue, rightValue) >= 0,
            ComparisonOperator.LessThan => CompareValues(leftValue, rightValue) < 0,
            ComparisonOperator.LessThanOrEqual => CompareValues(leftValue, rightValue) <= 0,
            ComparisonOperator.Contains => ContainsValue(leftValue, rightValue),
            ComparisonOperator.In => InValue(leftValue, rightValue),
            _ => false
        };
    }

    private bool EvaluateBinary(Row row, Expression binary)
    {
        var binData = binary.As<Expression.BinaryData>();

        var leftResult = EvaluateFilter(row, binData.Left);
        var rightResult = EvaluateFilter(row, binData.Right);

        return binData.Operator switch
        {
            LogicalOperator.And => leftResult && rightResult,
            LogicalOperator.Or => leftResult || rightResult,
            LogicalOperator.Not => !leftResult,
            _ => false
        };
    }

    private bool EvaluateUnary(Row row, Expression unary)
    {
        var unData = unary.As<Expression.UnaryData>();

        var result = EvaluateFilter(row, unData.Operand);

        return unData.Operator switch
        {
            LogicalOperator.Not => !result,
            _ => false
        };

    }


    private static object? GetFieldValue(Row row, Expression fieldExpr)
    {
        if (fieldExpr.Type == ExpressionType.FieldPath)
        {
            var segments = fieldExpr.As<ReadOnlyMemory<string>>().Span;
            if (segments.Length == 0)
                return null;

            // Start with the first segment to get the root field
            if (!row.Fields.TryGetValue(segments[0], out var currentValue))
                return null;

            // If there's only one segment, return the value
            if (segments.Length == 1)
                return currentValue;

            // For nested paths, traverse the object hierarchy
            for (int i = 1; i < segments.Length; i++)
            {
                if (currentValue == null)
                    return null;

                // Handle nested dictionary case
                if (currentValue is Dictionary<string, object> nestedDict)
                {
                    if (!nestedDict.TryGetValue(segments[i], out currentValue))
                        return null;
                }
                // Handle JsonData case - nested within Expression.JsonData
                else if (currentValue is Expression.JsonData jsonData)
                {
                    var jsonValue = jsonData.Value;
                    if (jsonValue is Dictionary<string, Expression> expressionDict)
                    {
                        if (!expressionDict.TryGetValue(segments[i], out var nextExpr))
                            return null;

                        currentValue = nextExpr.Type == ExpressionType.JsonValue
                            ? (nextExpr.As<Expression.JsonData>().Value)
                            : nextExpr;
                    }
                    else
                        return null;
                }
                // Handle Dictionary<string, Expression> case
                else if (currentValue is Dictionary<string, Expression> expressionDict)
                {
                    if (!expressionDict.TryGetValue(segments[i], out var nextExpr))
                        return null;

                    currentValue = nextExpr.Type == ExpressionType.JsonValue
                        ? (nextExpr.As<Expression.JsonData>().Value)
                        : nextExpr;
                }
                else
                {
                    // If we can't navigate further but still have segments, return null
                    return null;
                }
            }

            return currentValue;
        }
        return null;
    }

    private static object? GetLiteralValue(Expression literalExpr)
    {
        return literalExpr.Type switch
        {
            ExpressionType.Literal => ParseLiteralValue(literalExpr),
            ExpressionType.JsonValue => ParseJsonValue(literalExpr),
            _ => null
        };
    }

    private static object? ParseLiteralValue(Expression literal)
    {
        var litData = literal.As<Expression.LiteralData>();
        return litData.LiteralType switch
        {
            LiteralType.String => litData.Value,
            LiteralType.Number => ParseNumber(litData.Value),
            LiteralType.Boolean => bool.Parse(litData.Value),
            LiteralType.Null => null,
            LiteralType.Date => ParseDate(litData.Value),
            _ => litData.Value
        };
    }

    private static object? ParseJsonValue(Expression jsonExpr)
    {
        var jsonData = jsonExpr.As<Expression.JsonData>();
        return jsonData.ValueType switch
        {
            JsonValueType.String => jsonData.Value?.ToString(),
            JsonValueType.Number => ParseNumber(jsonData.Value?.ToString()),
            JsonValueType.Boolean => bool.Parse(jsonData.Value?.ToString() ?? "false"),
            JsonValueType.Null => null,
            _ => jsonData.Value
        };
    }

    private static object ParseNumber(string? value)
    {

        if (string.IsNullOrEmpty(value)) return 0;

        else if (int.TryParse(value, out var intVal)) return intVal;
        else if (double.TryParse(value, out var doubleVal)) return doubleVal;
        return 0;
    }

    private static object ParseDate(string? value)
    {
        //possible:
        //now-7-days  [days, weeks, months, years]
        //this-month [day, week, month, year]
        //a date value

        if (string.IsNullOrEmpty(value)) return DateTime.Now;

        if (value.StartsWith("now"))
        {
            var parts = value.Split('-');
            if (parts.Length > 1 && int.TryParse(parts[1], out var amount) && parts.Length > 2)
            {
                var timeUnit = parts[2].ToLowerInvariant();
                return timeUnit switch
                {
                    "days" => DateTime.Today.AddDays(-amount),
                    "weeks" => DateTime.Today.AddDays(-amount * 7),
                    "months" => DateTime.Today.AddMonths(-amount),
                    "years" => DateTime.Today.AddYears(-amount),
                    _ => DateTime.Today
                };
            }
        }
        else if (value.StartsWith("this-"))
        {
            var parts = value.Split('-');
            if (parts.Length > 1)
            {
                var timeUnit = parts[1].ToLowerInvariant();
                return timeUnit switch
                {
                    "day" => DateTime.Today,
                    "week" => DateTime.Today.AddDays(-(int)DateTime.Today.DayOfWeek),
                    "month" => new DateTime(DateTime.Today.Year, DateTime.Today.Month, 1),
                    "year" => new DateTime(DateTime.Today.Year, 1, 1),
                    _ => DateTime.Today
                };
            }
        }
        else if (DateTime.TryParse(value, out var dateValue))
        {
            return dateValue;
        }
        return DateTime.Now; // Default to now if parsing fails


    }


    private static int CompareValues(object? left, object? right)
    {
        if (left == null && right == null) return 0;
        if (left == null) return -1;
        if (right == null) return 1;

        // Try numeric comparison first
        if (IsNumeric(left) && IsNumeric(right))
        {
            var leftNum = Convert.ToDouble(left);
            var rightNum = Convert.ToDouble(right);
            return leftNum.CompareTo(rightNum);
        }
        //Try DateTime comparison
        if ((left is DateTime leftDate || left is string leftString && DateTime.TryParse(leftString, out leftDate)) && right is DateTime rightDate)
        {
            return leftDate.CompareTo(rightDate);
        }

        // Fall back to string comparison
        return string.Compare(left.ToString(), right.ToString(), StringComparison.OrdinalIgnoreCase);
    }

    private static bool ContainsValue(object? container, object? value)
    {
        return container is not null
            && value is not null
            && container.ToString()!.Contains(value.ToString()!, StringComparison.OrdinalIgnoreCase);
    }

    private static bool InValue(object? left, object? right)
    {
        if (left is null || right is null)
            return false;

        // Handle the case where right is an array of JsonValue expressions
        if (right is Expression[] jsonValues)
        {
            var leftStr = left.ToString();

            foreach (var expr in jsonValues)
            {
                if (expr.Type == ExpressionType.JsonValue)
                {
                    var jsonData = expr.As<Expression.JsonData>();
                    var valueStr = jsonData.Value?.ToString();

                    if (valueStr != null && string.Equals(leftStr, valueStr, StringComparison.OrdinalIgnoreCase))
                        return true;
                }
            }
        }

        return false;
    }

    private static bool IsNumeric(object value)
    {
        return value is int or long or float or double or decimal;
    }

    #endregion

    #region Debug Methods

    public void PrintTableContents(string tableName)
    {
        var database = server.GetCurrentDatabase();
        if (database == null)
        {
            Console.WriteLine("No current database selected");
            return;
        }

        if (!database.Tables.TryGetValue(tableName, out var table))
        {
            Console.WriteLine($"Table '{tableName}' does not exist");
            return;
        }

        Console.WriteLine($"\n=== Table: {tableName} ({table.Rows.Count} rows) ===");

        if (table.Rows.Count == 0)
        {
            Console.WriteLine("(empty)");
            return;
        }

        // Print headers
        var firstRow = table.Rows.Values.FirstOrDefault();
        if (firstRow == null)
        {
            Console.WriteLine("(empty)");
            return;
        }

        var headers = _first.Concat(firstRow.Fields.Keys).ToArray();
        Console.WriteLine(string.Join(" | ", headers.Select(h => h.PadRight(15))));
        Console.WriteLine(new string('-', headers.Length * 18));

        // Print rows
        foreach (var row in table.Rows.Values)
        {
            var values = new[] { row.Id.ToString() ?? "" }
                .Concat(headers.Skip(1).Select(h =>
                    row.Fields.TryGetValue(h, out var val) ? val?.ToString() ?? "null" : ""))
                .ToArray();

            Console.WriteLine(string.Join(" | ", values.Select(v => v.PadRight(15))));
        }
        Console.WriteLine();
    }

    public void PrintAllTables()
    {
        var database = server.GetCurrentDatabase();
        if (database == null)
        {
            Console.WriteLine("No current database selected");
            return;
        }

        Console.WriteLine($"\n=== DataStore Contents ({database.Tables.Count} tables) ===");
        foreach (var tableName in database.Tables.Keys)
        {
            PrintTableContents(tableName);
        }
    }

    #endregion
}
using System.Globalization;
using System.Linq.Expressions;

namespace SproutDB.Core.Linq;

internal static class SproutExpressionVisitor
{
    /// <param name="parameters">
    /// Collects the values as placeholders; null inlines them as literals.
    /// </param>
    internal static string ConvertWhere<T>(Expression<Func<T, bool>> predicate, ParameterCollector? parameters = null)
    {
        return VisitExpression(predicate.Body, parameters);
    }

    internal static List<string> ConvertSelect<T, TResult>(Expression<Func<T, TResult>> selector)
    {
        var body = selector.Body;

        // Strip Convert wrapper (boxing)
        if (body is UnaryExpression { NodeType: ExpressionType.Convert } convert)
            body = convert.Operand;

        // Single member: u => u.Name
        if (body is MemberExpression member)
            return [TypeMapper.ToColumnName(member.Member.Name)];

        // Anonymous type: u => new { u.Name, u.Age }
        if (body is NewExpression newExpr && newExpr.Arguments is not null)
        {
            var columns = new List<string>(newExpr.Arguments.Count);
            foreach (var arg in newExpr.Arguments)
            {
                if (arg is MemberExpression m)
                    columns.Add(TypeMapper.ToColumnName(m.Member.Name));
                else
                    throw new SproutQueryException($"Unsupported select expression: {arg}");
            }
            return columns;
        }

        throw new SproutQueryException($"Unsupported select expression type: {body.NodeType}");
    }

    internal static string ConvertOrderBy<T, TKey>(Expression<Func<T, TKey>> keySelector)
    {
        if (keySelector.Body is MemberExpression member)
            return TypeMapper.ToColumnName(member.Member.Name);

        throw new SproutQueryException($"OrderBy expression must be a property accessor, got: {keySelector.Body.NodeType}");
    }

    internal static string ConvertMemberName<T, TKey>(Expression<Func<T, TKey>> selector)
    {
        var body = selector.Body;

        // Strip Convert wrapper (boxing for Expression<Func<T, object>>)
        if (body is UnaryExpression { NodeType: ExpressionType.Convert } convert)
            body = convert.Operand;

        if (body is MemberExpression member)
            return TypeMapper.ToColumnName(member.Member.Name);

        throw new SproutQueryException($"Expression must be a property accessor, got: {body.NodeType}");
    }

    private static string VisitExpression(Expression expr, ParameterCollector? parameters)
    {
        return expr switch
        {
            BinaryExpression binary => VisitBinary(binary, parameters),
            UnaryExpression { NodeType: ExpressionType.Not } unary => VisitNot(unary, parameters),
            MethodCallExpression method => VisitMethodCall(method, parameters),
            _ => throw new SproutQueryException($"Unsupported expression type: {expr.NodeType}"),
        };
    }

    private static string VisitBinary(BinaryExpression binary, ParameterCollector? parameters)
    {
        if (binary.NodeType == ExpressionType.AndAlso)
            return $"{VisitExpression(binary.Left, parameters)} and {VisitExpression(binary.Right, parameters)}";

        if (binary.NodeType == ExpressionType.OrElse)
            return $"{VisitExpression(binary.Left, parameters)} or {VisitExpression(binary.Right, parameters)}";

        var column = ExtractColumnName(binary.Left);
        var raw = ExtractValue(binary.Right);

        // Handle null comparisons
        if (raw is null)
        {
            return binary.NodeType switch
            {
                ExpressionType.Equal => $"{column} is null",
                ExpressionType.NotEqual => $"{column} is not null",
                _ => throw new SproutQueryException("null can only be used with == or !="),
            };
        }

        var value = ParameterCollector.Render(raw, parameters);

        var op = binary.NodeType switch
        {
            ExpressionType.Equal => "=",
            ExpressionType.NotEqual => "!=",
            ExpressionType.GreaterThan => ">",
            ExpressionType.GreaterThanOrEqual => ">=",
            ExpressionType.LessThan => "<",
            ExpressionType.LessThanOrEqual => "<=",
            _ => throw new SproutQueryException($"Unsupported binary operator: {binary.NodeType}"),
        };

        return $"{column} {op} {value}";
    }

    private static string VisitNot(UnaryExpression unary, ParameterCollector? parameters)
    {
        return $"not {VisitExpression(unary.Operand, parameters)}";
    }

    private static string VisitMethodCall(MethodCallExpression method, ParameterCollector? parameters)
    {
        if (method.Object is MemberExpression member && method.Method.DeclaringType == typeof(string))
        {
            var column = TypeMapper.ToColumnName(member.Member.Name);
            var arg = ParameterCollector.Render(ExtractValue(method.Arguments[0]), parameters);

            return method.Method.Name switch
            {
                "Contains" => $"{column} contains {arg}",
                "StartsWith" => $"{column} starts {arg}",
                "EndsWith" => $"{column} ends {arg}",
                _ => throw new SproutQueryException($"Unsupported string method: {method.Method.Name}"),
            };
        }

        throw new SproutQueryException($"Unsupported method call: {method.Method.Name}");
    }

    private static string ExtractColumnName(Expression expr)
    {
        if (expr is MemberExpression member)
            return TypeMapper.ToColumnName(member.Member.Name);

        if (expr is UnaryExpression { NodeType: ExpressionType.Convert } convert)
            return ExtractColumnName(convert.Operand);

        throw new SproutQueryException($"Expected property accessor, got: {expr.NodeType}");
    }

    /// <summary>
    /// Evaluates a constant or captured variable to its CLR value.
    /// </summary>
    private static object? ExtractValue(Expression expr)
    {
        if (expr is UnaryExpression { NodeType: ExpressionType.Convert } convert)
            return ExtractValue(convert.Operand);

        if (expr is ConstantExpression constant)
            return constant.Value;

        // Captured variable (closure)
        if (expr is MemberExpression)
            return Expression.Lambda(expr).Compile().DynamicInvoke();

        throw new SproutQueryException($"Expected constant or captured variable, got: {expr.NodeType}");
    }
}

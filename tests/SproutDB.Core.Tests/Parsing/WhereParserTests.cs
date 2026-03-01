using SproutDB.Core.Parsing;

namespace SproutDB.Core.Tests.Parsing;

public class WhereParserTests
{
    private static CompareNode AssertCompare(GetQuery q)
    {
        Assert.NotNull(q.Where);
        return Assert.IsType<CompareNode>(q.Where);
    }

    private static CompareNode ParseCompare(string input)
    {
        var result = QueryParser.Parse(input);
        Assert.True(result.Success);
        return AssertCompare(Assert.IsType<GetQuery>(result.Query));
    }

    [Fact]
    public void Where_Equal_Integer()
    {
        var c = ParseCompare("get users where age = 28");

        Assert.Equal("age", c.Column);
        Assert.Equal(CompareOp.Equal, c.Operator);
        Assert.Equal("28", c.Value);
    }

    [Fact]
    public void Where_GreaterThan()
    {
        var c = ParseCompare("get users where age > 30");

        Assert.Equal(CompareOp.GreaterThan, c.Operator);
        Assert.Equal("30", c.Value);
    }

    [Fact]
    public void Where_GreaterThanOrEqual()
    {
        var c = ParseCompare("get users where age >= 18");

        Assert.Equal(CompareOp.GreaterThanOrEqual, c.Operator);
    }

    [Fact]
    public void Where_LessThan()
    {
        var c = ParseCompare("get users where age < 18");

        Assert.Equal(CompareOp.LessThan, c.Operator);
    }

    [Fact]
    public void Where_LessThanOrEqual()
    {
        var c = ParseCompare("get users where age <= 30");

        Assert.Equal(CompareOp.LessThanOrEqual, c.Operator);
    }

    [Fact]
    public void Where_NotEqual()
    {
        var c = ParseCompare("get users where age != 18");

        Assert.Equal(CompareOp.NotEqual, c.Operator);
    }

    [Fact]
    public void Where_StringValue()
    {
        var c = ParseCompare("get users where name = 'John'");

        Assert.Equal("name", c.Column);
        Assert.Equal("John", c.Value);
    }

    [Fact]
    public void Where_BoolValue()
    {
        var c = ParseCompare("get users where active = true");

        Assert.Equal("true", c.Value);
    }

    [Fact]
    public void Where_NegativeValue()
    {
        var c = ParseCompare("get users where score > -10");

        Assert.Equal("-10", c.Value);
    }

    [Fact]
    public void Where_FloatValue()
    {
        var c = ParseCompare("get users where rating > 4.5");

        Assert.Equal("4.5", c.Value);
    }

    [Fact]
    public void Where_WithSelect()
    {
        var result = QueryParser.Parse("get users select name, age where age > 30");

        Assert.True(result.Success);
        var q = Assert.IsType<GetQuery>(result.Query);
        Assert.NotNull(q.Select);
        Assert.Equal(2, q.Select.Count);
        var c = AssertCompare(q);
        Assert.Equal("age", c.Column);
    }

    [Fact]
    public void Where_CaseInsensitive()
    {
        var c = ParseCompare("GET Users WHERE Age > 30");

        Assert.Equal("age", c.Column);
    }

    [Fact]
    public void NoWhere_NullClause()
    {
        var result = QueryParser.Parse("get users");

        Assert.True(result.Success);
        var q = Assert.IsType<GetQuery>(result.Query);
        Assert.Null(q.Where);
    }

    [Fact]
    public void Where_MissingOperator_Error()
    {
        var result = QueryParser.Parse("get users where age 30");

        Assert.False(result.Success);
        Assert.Contains("comparison operator", result.Errors![0].Message);
    }

    [Fact]
    public void Where_MissingValue_Error()
    {
        var result = QueryParser.Parse("get users where age >");

        Assert.False(result.Success);
        Assert.Contains("expected a value", result.Errors![0].Message);
    }

    [Fact]
    public void Where_MissingColumn_Error()
    {
        var result = QueryParser.Parse("get users where = 30");

        Assert.False(result.Success);
        Assert.Contains("column name", result.Errors![0].Message);
    }

    // ── String operators ────────────────────────────────────────

    [Fact]
    public void Where_Contains()
    {
        var c = ParseCompare("get users where email contains '@gmail'");

        Assert.Equal("email", c.Column);
        Assert.Equal(CompareOp.Contains, c.Operator);
        Assert.Equal("@gmail", c.Value);
    }

    [Fact]
    public void Where_StartsWith()
    {
        var c = ParseCompare("get users where name starts 'Jo'");

        Assert.Equal(CompareOp.StartsWith, c.Operator);
        Assert.Equal("Jo", c.Value);
    }

    [Fact]
    public void Where_EndsWith()
    {
        var c = ParseCompare("get users where name ends 'son'");

        Assert.Equal(CompareOp.EndsWith, c.Operator);
        Assert.Equal("son", c.Value);
    }

    [Fact]
    public void Where_Contains_CaseInsensitive()
    {
        var c = ParseCompare("get users where email CONTAINS '@gmail'");

        Assert.Equal(CompareOp.Contains, c.Operator);
    }

    [Fact]
    public void Where_Contains_MissingValue_Error()
    {
        var result = QueryParser.Parse("get users where email contains");

        Assert.False(result.Success);
        Assert.Contains("expected a value", result.Errors![0].Message);
    }

    // ── Between ─────────────────────────────────────────────

    [Fact]
    public void Where_Between()
    {
        var c = ParseCompare("get users where age between 18 and 30");

        Assert.Equal(CompareOp.Between, c.Operator);
        Assert.Equal("18", c.Value);
        Assert.Equal("30", c.Value2);
    }

    [Fact]
    public void Where_NotBetween()
    {
        var c = ParseCompare("get users where age not between 18 and 30");

        Assert.Equal(CompareOp.NotBetween, c.Operator);
        Assert.Equal("18", c.Value);
        Assert.Equal("30", c.Value2);
    }

    [Fact]
    public void Where_Between_CaseInsensitive()
    {
        var c = ParseCompare("get users where age BETWEEN 10 AND 50");

        Assert.Equal(CompareOp.Between, c.Operator);
    }

    [Fact]
    public void Where_Between_MissingAnd_Error()
    {
        var result = QueryParser.Parse("get users where age between 18 30");

        Assert.False(result.Success);
        Assert.Contains("'and'", result.Errors![0].Message);
    }

    [Fact]
    public void Where_Between_MissingSecondValue_Error()
    {
        var result = QueryParser.Parse("get users where age between 18 and");

        Assert.False(result.Success);
        Assert.Contains("expected a value", result.Errors![0].Message);
    }

    [Fact]
    public void Where_Between_StringValues()
    {
        var c = ParseCompare("get users where created between '2025-01-01' and '2025-12-31'");

        Assert.Equal("2025-01-01", c.Value);
        Assert.Equal("2025-12-31", c.Value2);
    }

    // ── AND ─────────────────────────────────────────────────

    [Fact]
    public void Where_And()
    {
        var result = QueryParser.Parse("get users where age >= 18 and active = true");

        Assert.True(result.Success);
        var q = Assert.IsType<GetQuery>(result.Query);
        var logical = Assert.IsType<LogicalNode>(q.Where);
        Assert.Equal(LogicalOp.And, logical.Op);

        var left = Assert.IsType<CompareNode>(logical.Left);
        Assert.Equal("age", left.Column);
        Assert.Equal(CompareOp.GreaterThanOrEqual, left.Operator);

        var right = Assert.IsType<CompareNode>(logical.Right);
        Assert.Equal("active", right.Column);
        Assert.Equal("true", right.Value);
    }

    [Fact]
    public void Where_And_CaseInsensitive()
    {
        var result = QueryParser.Parse("get users where age >= 18 AND active = true");

        Assert.True(result.Success);
        var q = Assert.IsType<GetQuery>(result.Query);
        Assert.IsType<LogicalNode>(q.Where);
    }

    [Fact]
    public void Where_And_Multiple()
    {
        // Three conditions → left-associative: AND(AND(a,b),c)
        var result = QueryParser.Parse("get users where a = 1 and b = 2 and c = 3");

        Assert.True(result.Success);
        var q = Assert.IsType<GetQuery>(result.Query);
        var outer = Assert.IsType<LogicalNode>(q.Where);
        Assert.Equal(LogicalOp.And, outer.Op);

        var inner = Assert.IsType<LogicalNode>(outer.Left);
        Assert.Equal(LogicalOp.And, inner.Op);

        Assert.Equal("a", Assert.IsType<CompareNode>(inner.Left).Column);
        Assert.Equal("b", Assert.IsType<CompareNode>(inner.Right).Column);
        Assert.Equal("c", Assert.IsType<CompareNode>(outer.Right).Column);
    }

    // ── OR ──────────────────────────────────────────────────

    [Fact]
    public void Where_Or()
    {
        var result = QueryParser.Parse("get users where role = 'admin' or role = 'mod'");

        Assert.True(result.Success);
        var q = Assert.IsType<GetQuery>(result.Query);
        var logical = Assert.IsType<LogicalNode>(q.Where);
        Assert.Equal(LogicalOp.Or, logical.Op);
    }

    // ── NOT ─────────────────────────────────────────────────

    [Fact]
    public void Where_Not()
    {
        var result = QueryParser.Parse("get users where not active = true");

        Assert.True(result.Success);
        var q = Assert.IsType<GetQuery>(result.Query);
        var not = Assert.IsType<NotNode>(q.Where);
        var inner = Assert.IsType<CompareNode>(not.Inner);
        Assert.Equal("active", inner.Column);
        Assert.Equal("true", inner.Value);
    }

    [Fact]
    public void Where_Not_With_And()
    {
        // NOT binds tighter: NOT(age>18) AND name='x'
        var result = QueryParser.Parse("get users where not age > 18 and name = 'x'");

        Assert.True(result.Success);
        var q = Assert.IsType<GetQuery>(result.Query);
        var logical = Assert.IsType<LogicalNode>(q.Where);
        Assert.Equal(LogicalOp.And, logical.Op);

        var notLeft = Assert.IsType<NotNode>(logical.Left);
        var compare = Assert.IsType<CompareNode>(notLeft.Inner);
        Assert.Equal("age", compare.Column);

        Assert.IsType<CompareNode>(logical.Right);
    }

    // ── IS NULL / IS NOT NULL ───────────────────────────────

    [Fact]
    public void Where_IsNull()
    {
        var result = QueryParser.Parse("get users where email is null");

        Assert.True(result.Success);
        var q = Assert.IsType<GetQuery>(result.Query);
        var nc = Assert.IsType<NullCheckNode>(q.Where);
        Assert.Equal("email", nc.Column);
        Assert.False(nc.IsNot);
    }

    [Fact]
    public void Where_IsNotNull()
    {
        var result = QueryParser.Parse("get users where email is not null");

        Assert.True(result.Success);
        var q = Assert.IsType<GetQuery>(result.Query);
        var nc = Assert.IsType<NullCheckNode>(q.Where);
        Assert.Equal("email", nc.Column);
        Assert.True(nc.IsNot);
    }

    // ── Precedence ──────────────────────────────────────────

    [Fact]
    public void Where_And_Or_Precedence()
    {
        // a=1 or b=2 and c=3 → OR(a=1, AND(b=2, c=3))
        var result = QueryParser.Parse("get users where a = 1 or b = 2 and c = 3");

        Assert.True(result.Success);
        var q = Assert.IsType<GetQuery>(result.Query);
        var or = Assert.IsType<LogicalNode>(q.Where);
        Assert.Equal(LogicalOp.Or, or.Op);

        Assert.IsType<CompareNode>(or.Left);

        var and = Assert.IsType<LogicalNode>(or.Right);
        Assert.Equal(LogicalOp.And, and.Op);
        Assert.Equal("b", Assert.IsType<CompareNode>(and.Left).Column);
        Assert.Equal("c", Assert.IsType<CompareNode>(and.Right).Column);
    }

    // ── IN / NOT IN ──────────────────────────────────────────

    [Fact]
    public void Where_In()
    {
        var result = QueryParser.Parse("get users where role in ['admin', 'mod']");

        Assert.True(result.Success);
        var q = Assert.IsType<GetQuery>(result.Query);
        var inNode = Assert.IsType<InNode>(q.Where);
        Assert.Equal("role", inNode.Column);
        Assert.False(inNode.IsNot);
        Assert.Equal(2, inNode.Values.Count);
        Assert.Equal("admin", inNode.Values[0]);
        Assert.Equal("mod", inNode.Values[1]);
    }

    [Fact]
    public void Where_NotIn()
    {
        var result = QueryParser.Parse("get users where age not in [28, 35]");

        Assert.True(result.Success);
        var q = Assert.IsType<GetQuery>(result.Query);
        var inNode = Assert.IsType<InNode>(q.Where);
        Assert.Equal("age", inNode.Column);
        Assert.True(inNode.IsNot);
        Assert.Equal(2, inNode.Values.Count);
        Assert.Equal("28", inNode.Values[0]);
        Assert.Equal("35", inNode.Values[1]);
    }

    [Fact]
    public void Where_In_SingleValue()
    {
        var result = QueryParser.Parse("get users where status in ['active']");

        Assert.True(result.Success);
        var q = Assert.IsType<GetQuery>(result.Query);
        var inNode = Assert.IsType<InNode>(q.Where);
        Assert.Single(inNode.Values);
        Assert.Equal("active", inNode.Values[0]);
    }

    [Fact]
    public void Where_In_CaseInsensitive()
    {
        var result = QueryParser.Parse("get users where role IN ['admin']");

        Assert.True(result.Success);
        var q = Assert.IsType<GetQuery>(result.Query);
        Assert.IsType<InNode>(q.Where);
    }

    [Fact]
    public void Where_In_MissingBracket_Error()
    {
        var result = QueryParser.Parse("get users where role in 'admin'");

        Assert.False(result.Success);
        Assert.Contains("'['", result.Errors![0].Message);
    }

    [Fact]
    public void Where_In_EmptyList_Error()
    {
        var result = QueryParser.Parse("get users where role in []");

        Assert.False(result.Success);
        Assert.Contains("at least one value", result.Errors![0].Message);
    }

    [Fact]
    public void Where_In_With_And()
    {
        var result = QueryParser.Parse("get users where role in ['admin'] and active = true");

        Assert.True(result.Success);
        var q = Assert.IsType<GetQuery>(result.Query);
        var logical = Assert.IsType<LogicalNode>(q.Where);
        Assert.Equal(LogicalOp.And, logical.Op);
        Assert.IsType<InNode>(logical.Left);
        Assert.IsType<CompareNode>(logical.Right);
    }

    // ── Between + Logical AND ───────────────────────────────

    [Fact]
    public void Where_Between_And_Logical()
    {
        // 'and' in between is consumed by ParseComparison → no conflict
        var result = QueryParser.Parse("get users where age between 18 and 30 and name = 'x'");

        Assert.True(result.Success);
        var q = Assert.IsType<GetQuery>(result.Query);
        var logical = Assert.IsType<LogicalNode>(q.Where);
        Assert.Equal(LogicalOp.And, logical.Op);

        var between = Assert.IsType<CompareNode>(logical.Left);
        Assert.Equal(CompareOp.Between, between.Operator);
        Assert.Equal("18", between.Value);
        Assert.Equal("30", between.Value2);

        var right = Assert.IsType<CompareNode>(logical.Right);
        Assert.Equal("name", right.Column);
    }

    // ── ORDER BY ────────────────────────────────────────────

    [Fact]
    public void OrderBy_Single()
    {
        var result = QueryParser.Parse("get users order by name");

        Assert.True(result.Success);
        var q = Assert.IsType<GetQuery>(result.Query);
        Assert.NotNull(q.OrderBy);
        Assert.Single(q.OrderBy);
        Assert.Equal("name", q.OrderBy[0].Name);
        Assert.False(q.OrderBy[0].Descending);
    }

    [Fact]
    public void OrderBy_Desc()
    {
        var result = QueryParser.Parse("get users order by age desc");

        Assert.True(result.Success);
        var q = Assert.IsType<GetQuery>(result.Query);
        Assert.NotNull(q.OrderBy);
        Assert.Single(q.OrderBy);
        Assert.Equal("age", q.OrderBy[0].Name);
        Assert.True(q.OrderBy[0].Descending);
    }

    [Fact]
    public void OrderBy_Multiple()
    {
        var result = QueryParser.Parse("get users order by created desc, name");

        Assert.True(result.Success);
        var q = Assert.IsType<GetQuery>(result.Query);
        Assert.NotNull(q.OrderBy);
        Assert.Equal(2, q.OrderBy.Count);
        Assert.Equal("created", q.OrderBy[0].Name);
        Assert.True(q.OrderBy[0].Descending);
        Assert.Equal("name", q.OrderBy[1].Name);
        Assert.False(q.OrderBy[1].Descending);
    }

    [Fact]
    public void OrderBy_CaseInsensitive()
    {
        var result = QueryParser.Parse("GET Users ORDER BY Name DESC");

        Assert.True(result.Success);
        var q = Assert.IsType<GetQuery>(result.Query);
        Assert.NotNull(q.OrderBy);
        Assert.Equal("name", q.OrderBy[0].Name);
        Assert.True(q.OrderBy[0].Descending);
    }

    [Fact]
    public void OrderBy_With_Where()
    {
        var result = QueryParser.Parse("get users where age > 18 order by name");

        Assert.True(result.Success);
        var q = Assert.IsType<GetQuery>(result.Query);
        Assert.NotNull(q.Where);
        Assert.NotNull(q.OrderBy);
        Assert.Equal("name", q.OrderBy[0].Name);
    }

    [Fact]
    public void OrderBy_MissingBy_Error()
    {
        var result = QueryParser.Parse("get users order name");

        Assert.False(result.Success);
        Assert.Contains("'by'", result.Errors![0].Message);
    }

    // ── LIMIT ───────────────────────────────────────────────

    [Fact]
    public void Limit_Simple()
    {
        var result = QueryParser.Parse("get users limit 10");

        Assert.True(result.Success);
        var q = Assert.IsType<GetQuery>(result.Query);
        Assert.Equal(10, q.Limit);
    }

    [Fact]
    public void Limit_With_OrderBy()
    {
        var result = QueryParser.Parse("get users order by name limit 5");

        Assert.True(result.Success);
        var q = Assert.IsType<GetQuery>(result.Query);
        Assert.NotNull(q.OrderBy);
        Assert.Equal(5, q.Limit);
    }

    [Fact]
    public void Limit_MissingValue_Error()
    {
        var result = QueryParser.Parse("get users limit");

        Assert.False(result.Success);
        Assert.Contains("integer", result.Errors![0].Message);
    }

    [Fact]
    public void Limit_CaseInsensitive()
    {
        var result = QueryParser.Parse("get users LIMIT 10");

        Assert.True(result.Success);
        var q = Assert.IsType<GetQuery>(result.Query);
        Assert.Equal(10, q.Limit);
    }

    // ── COUNT ───────────────────────────────────────────────

    [Fact]
    public void Count_Simple()
    {
        var result = QueryParser.Parse("get users count");

        Assert.True(result.Success);
        var q = Assert.IsType<GetQuery>(result.Query);
        Assert.True(q.IsCount);
    }

    [Fact]
    public void Count_With_Where()
    {
        var result = QueryParser.Parse("get users where active = true count");

        Assert.True(result.Success);
        var q = Assert.IsType<GetQuery>(result.Query);
        Assert.NotNull(q.Where);
        Assert.True(q.IsCount);
    }

    [Fact]
    public void Count_CaseInsensitive()
    {
        var result = QueryParser.Parse("get users COUNT");

        Assert.True(result.Success);
        var q = Assert.IsType<GetQuery>(result.Query);
        Assert.True(q.IsCount);
    }

    // ── DISTINCT ────────────────────────────────────────────

    [Fact]
    public void Distinct_Simple()
    {
        var result = QueryParser.Parse("get users select city distinct");

        Assert.True(result.Success);
        var q = Assert.IsType<GetQuery>(result.Query);
        Assert.True(q.IsDistinct);
        Assert.NotNull(q.Select);
        Assert.Equal("city", q.Select[0].Name);
    }

    [Fact]
    public void Distinct_With_Where()
    {
        var result = QueryParser.Parse("get users select role distinct where active = true");

        Assert.True(result.Success);
        var q = Assert.IsType<GetQuery>(result.Query);
        Assert.True(q.IsDistinct);
        Assert.NotNull(q.Where);
    }

    [Fact]
    public void Distinct_Without_Select_Error()
    {
        var result = QueryParser.Parse("get users distinct");

        Assert.False(result.Success);
        Assert.Contains("'select'", result.Errors![0].Message);
    }

    [Fact]
    public void Distinct_CaseInsensitive()
    {
        var result = QueryParser.Parse("get users select city DISTINCT");

        Assert.True(result.Success);
        var q = Assert.IsType<GetQuery>(result.Query);
        Assert.True(q.IsDistinct);
    }

    // ── PAGE / SIZE ─────────────────────────────────────────

    [Fact]
    public void Page_Simple()
    {
        var result = QueryParser.Parse("get users page 2 size 50");

        Assert.True(result.Success);
        var q = Assert.IsType<GetQuery>(result.Query);
        Assert.Equal(2, q.Page);
        Assert.Equal(50, q.Size);
    }

    [Fact]
    public void Page_With_Where()
    {
        var result = QueryParser.Parse("get users where age > 18 page 3 size 10");

        Assert.True(result.Success);
        var q = Assert.IsType<GetQuery>(result.Query);
        Assert.NotNull(q.Where);
        Assert.Equal(3, q.Page);
        Assert.Equal(10, q.Size);
    }

    [Fact]
    public void Page_With_OrderBy()
    {
        var result = QueryParser.Parse("get users order by name page 1 size 25");

        Assert.True(result.Success);
        var q = Assert.IsType<GetQuery>(result.Query);
        Assert.NotNull(q.OrderBy);
        Assert.Equal(1, q.Page);
        Assert.Equal(25, q.Size);
    }

    [Fact]
    public void Page_CaseInsensitive()
    {
        var result = QueryParser.Parse("get users PAGE 1 SIZE 100");

        Assert.True(result.Success);
        var q = Assert.IsType<GetQuery>(result.Query);
        Assert.Equal(1, q.Page);
        Assert.Equal(100, q.Size);
    }

    [Fact]
    public void Page_MissingSize_Error()
    {
        var result = QueryParser.Parse("get users page 1");

        Assert.False(result.Success);
        Assert.Contains("'size'", result.Errors![0].Message);
    }

    [Fact]
    public void Page_MissingPageNumber_Error()
    {
        var result = QueryParser.Parse("get users page");

        Assert.False(result.Success);
        Assert.Contains("integer", result.Errors![0].Message);
    }

    [Fact]
    public void Page_MissingSizeNumber_Error()
    {
        var result = QueryParser.Parse("get users page 1 size");

        Assert.False(result.Success);
        Assert.Contains("integer", result.Errors![0].Message);
    }

    // ── AGGREGATE ───────────────────────────────────────────

    [Fact]
    public void Aggregate_Sum()
    {
        var result = QueryParser.Parse("get orders sum amount");

        Assert.True(result.Success);
        var q = Assert.IsType<GetQuery>(result.Query);
        Assert.Equal(AggregateFunction.Sum, q.Aggregate);
        Assert.Equal("amount", q.AggregateColumn);
        Assert.Null(q.AggregateAlias);
    }

    [Fact]
    public void Aggregate_Avg()
    {
        var result = QueryParser.Parse("get orders avg amount");

        Assert.True(result.Success);
        var q = Assert.IsType<GetQuery>(result.Query);
        Assert.Equal(AggregateFunction.Avg, q.Aggregate);
    }

    [Fact]
    public void Aggregate_Min()
    {
        var result = QueryParser.Parse("get orders min amount");

        Assert.True(result.Success);
        var q = Assert.IsType<GetQuery>(result.Query);
        Assert.Equal(AggregateFunction.Min, q.Aggregate);
    }

    [Fact]
    public void Aggregate_Max()
    {
        var result = QueryParser.Parse("get orders max amount");

        Assert.True(result.Success);
        var q = Assert.IsType<GetQuery>(result.Query);
        Assert.Equal(AggregateFunction.Max, q.Aggregate);
    }

    [Fact]
    public void Aggregate_WithAlias()
    {
        var result = QueryParser.Parse("get orders sum amount as total_revenue");

        Assert.True(result.Success);
        var q = Assert.IsType<GetQuery>(result.Query);
        Assert.Equal(AggregateFunction.Sum, q.Aggregate);
        Assert.Equal("amount", q.AggregateColumn);
        Assert.Equal("total_revenue", q.AggregateAlias);
    }

    [Fact]
    public void Aggregate_WithWhere()
    {
        var result = QueryParser.Parse("get orders sum amount where status = 'completed'");

        Assert.True(result.Success);
        var q = Assert.IsType<GetQuery>(result.Query);
        Assert.Equal(AggregateFunction.Sum, q.Aggregate);
        Assert.NotNull(q.Where);
    }

    [Fact]
    public void Aggregate_WithAliasAndWhere()
    {
        var result = QueryParser.Parse("get orders avg amount as average_order_value where status = 'completed'");

        Assert.True(result.Success);
        var q = Assert.IsType<GetQuery>(result.Query);
        Assert.Equal(AggregateFunction.Avg, q.Aggregate);
        Assert.Equal("average_order_value", q.AggregateAlias);
        Assert.NotNull(q.Where);
    }

    [Fact]
    public void Aggregate_CaseInsensitive()
    {
        var result = QueryParser.Parse("GET orders SUM Amount AS Total");

        Assert.True(result.Success);
        var q = Assert.IsType<GetQuery>(result.Query);
        Assert.Equal(AggregateFunction.Sum, q.Aggregate);
        Assert.Equal("amount", q.AggregateColumn);
        Assert.Equal("total", q.AggregateAlias);
    }

    [Fact]
    public void Aggregate_TableNamedSum_NoConflict()
    {
        // "get summary" should not trigger aggregate — no column after "sum"
        var result = QueryParser.Parse("get summary");

        Assert.True(result.Success);
        var q = Assert.IsType<GetQuery>(result.Query);
        Assert.Equal("summary", q.Table);
        Assert.Null(q.Aggregate);
    }

    // ── FOLLOW (join) ────────────────────────────────────────

    [Fact]
    public void Follow_Basic()
    {
        var result = QueryParser.Parse("get users follow users.id -> orders.user_id as orders");

        Assert.True(result.Success);
        var q = Assert.IsType<GetQuery>(result.Query);
        Assert.NotNull(q.Follow);
        Assert.Single(q.Follow);
        var f = q.Follow[0];
        Assert.Equal("users", f.SourceTable);
        Assert.Equal("id", f.SourceColumn);
        Assert.Equal("orders", f.TargetTable);
        Assert.Equal("user_id", f.TargetColumn);
        Assert.Equal("orders", f.Alias);
        Assert.Null(f.Where);
    }

    [Fact]
    public void Follow_WithWhere()
    {
        var result = QueryParser.Parse(
            "get users where active = true follow users.id -> orders.user_id as orders where status = 'completed'");

        Assert.True(result.Success);
        var q = Assert.IsType<GetQuery>(result.Query);
        Assert.NotNull(q.Where);
        Assert.NotNull(q.Follow);
        Assert.Single(q.Follow);
        var f = q.Follow[0];
        Assert.Equal("orders", f.TargetTable);
        Assert.NotNull(f.Where);
        var cmp = Assert.IsType<CompareNode>(f.Where);
        Assert.Equal("status", cmp.Column);
        Assert.Equal("completed", cmp.Value);
    }

    [Fact]
    public void Follow_Multiple()
    {
        var result = QueryParser.Parse(
            "get users follow users.id -> orders.user_id as orders follow orders.product_id -> products.id as product");

        Assert.True(result.Success);
        var q = Assert.IsType<GetQuery>(result.Query);
        Assert.NotNull(q.Follow);
        Assert.Equal(2, q.Follow.Count);
        Assert.Equal("orders", q.Follow[0].Alias);
        Assert.Equal("orders", q.Follow[0].TargetTable);
        Assert.Equal("product", q.Follow[1].Alias);
        Assert.Equal("products", q.Follow[1].TargetTable);
    }

    [Fact]
    public void Follow_Multiple_WithWhere()
    {
        var result = QueryParser.Parse(
            "get users follow users.id -> orders.user_id as orders where status = 'completed' follow orders.product_id -> products.id as product");

        Assert.True(result.Success);
        var q = Assert.IsType<GetQuery>(result.Query);
        Assert.NotNull(q.Follow);
        Assert.Equal(2, q.Follow.Count);
        Assert.NotNull(q.Follow[0].Where);
        Assert.Null(q.Follow[1].Where);
    }

    [Fact]
    public void Follow_CaseInsensitive()
    {
        var result = QueryParser.Parse("GET Users FOLLOW Users.Id -> Orders.User_Id AS MyOrders");

        Assert.True(result.Success);
        var q = Assert.IsType<GetQuery>(result.Query);
        Assert.NotNull(q.Follow);
        Assert.Equal("myorders", q.Follow[0].Alias);
    }

    [Fact]
    public void Follow_MissingArrow_Error()
    {
        var result = QueryParser.Parse("get users follow users.id orders.user_id as orders");

        Assert.False(result.Success);
    }

    [Fact]
    public void Follow_MissingAs_Error()
    {
        var result = QueryParser.Parse("get users follow users.id -> orders.user_id orders");

        Assert.False(result.Success);
    }

    [Fact]
    public void Follow_MissingDot_Error()
    {
        var result = QueryParser.Parse("get users follow users_id -> orders.user_id as orders");

        Assert.False(result.Success);
    }

    [Fact]
    public void Follow_NoConflict_WithSelect()
    {
        var result = QueryParser.Parse("get users select name, email follow users.id -> orders.user_id as orders");

        Assert.True(result.Success);
        var q = Assert.IsType<GetQuery>(result.Query);
        Assert.NotNull(q.Select);
        Assert.Equal(2, q.Select.Count);
        Assert.NotNull(q.Follow);
    }

    // ── GROUP BY ─────────────────────────────────────────────

    [Fact]
    public void GroupBy_WithAggregate()
    {
        var result = QueryParser.Parse("get orders sum amount as revenue group by status");

        Assert.True(result.Success);
        var q = Assert.IsType<GetQuery>(result.Query);
        Assert.Equal(AggregateFunction.Sum, q.Aggregate);
        Assert.Equal("amount", q.AggregateColumn);
        Assert.Equal("revenue", q.AggregateAlias);
        Assert.NotNull(q.GroupBy);
        Assert.Single(q.GroupBy);
        Assert.Equal("status", q.GroupBy[0].Name);
    }

    [Fact]
    public void GroupBy_WithCount()
    {
        var result = QueryParser.Parse("get orders count group by city");

        Assert.True(result.Success);
        var q = Assert.IsType<GetQuery>(result.Query);
        Assert.True(q.IsCount);
        Assert.NotNull(q.GroupBy);
        Assert.Single(q.GroupBy);
        Assert.Equal("city", q.GroupBy[0].Name);
    }

    [Fact]
    public void GroupBy_MultipleColumns()
    {
        var result = QueryParser.Parse("get orders count group by status, city");

        Assert.True(result.Success);
        var q = Assert.IsType<GetQuery>(result.Query);
        Assert.NotNull(q.GroupBy);
        Assert.Equal(2, q.GroupBy.Count);
        Assert.Equal("status", q.GroupBy[0].Name);
        Assert.Equal("city", q.GroupBy[1].Name);
    }

    [Fact]
    public void GroupBy_WithOrderByAndLimit()
    {
        var result = QueryParser.Parse("get orders avg amount as avg_amount group by customer_id order by avg_amount desc limit 10");

        Assert.True(result.Success);
        var q = Assert.IsType<GetQuery>(result.Query);
        Assert.Equal(AggregateFunction.Avg, q.Aggregate);
        Assert.NotNull(q.GroupBy);
        Assert.Equal("customer_id", q.GroupBy[0].Name);
        Assert.NotNull(q.OrderBy);
        Assert.Equal("avg_amount", q.OrderBy[0].Name);
        Assert.True(q.OrderBy[0].Descending);
        Assert.Equal(10, q.Limit);
    }

    [Fact]
    public void GroupBy_WithWhere()
    {
        var result = QueryParser.Parse("get orders sum amount as revenue where status != 'cancelled' group by city");

        Assert.True(result.Success);
        var q = Assert.IsType<GetQuery>(result.Query);
        Assert.NotNull(q.Where);
        Assert.NotNull(q.GroupBy);
        Assert.Equal("city", q.GroupBy[0].Name);
    }

    [Fact]
    public void GroupBy_CaseInsensitive()
    {
        var result = QueryParser.Parse("GET Orders COUNT GROUP BY Status");

        Assert.True(result.Success);
        var q = Assert.IsType<GetQuery>(result.Query);
        Assert.True(q.IsCount);
        Assert.NotNull(q.GroupBy);
        Assert.Equal("status", q.GroupBy[0].Name);
    }

    [Fact]
    public void GroupBy_MissingBy_Error()
    {
        var result = QueryParser.Parse("get orders count group status");

        Assert.False(result.Success);
    }

    [Fact]
    public void GroupBy_TableNamedGroup_NoConflict()
    {
        // "get groups" should not trigger group by parsing
        var result = QueryParser.Parse("get groups");

        Assert.True(result.Success);
        var q = Assert.IsType<GetQuery>(result.Query);
        Assert.Equal("groups", q.Table);
        Assert.Null(q.GroupBy);
    }

    // ── COMPUTED FIELDS ──────────────────────────────────────

    [Fact]
    public void Computed_MultiplyLiteral()
    {
        var result = QueryParser.Parse("get orders select amount, amount * 0.19 as tax");

        Assert.True(result.Success);
        var q = Assert.IsType<GetQuery>(result.Query);
        Assert.NotNull(q.Select);
        Assert.Single(q.Select);
        Assert.Equal("amount", q.Select[0].Name);

        Assert.NotNull(q.ComputedSelect);
        Assert.Single(q.ComputedSelect);
        var c = q.ComputedSelect[0];
        Assert.Equal("amount", c.LeftColumn);
        Assert.Equal(ArithmeticOp.Multiply, c.Operator);
        Assert.Equal(0.19, c.RightLiteral);
        Assert.Null(c.RightColumn);
        Assert.Equal("tax", c.Alias);
    }

    [Fact]
    public void Computed_MultiplyColumns()
    {
        var result = QueryParser.Parse("get orders select name, price, quantity, price * quantity as total");

        Assert.True(result.Success);
        var q = Assert.IsType<GetQuery>(result.Query);
        Assert.NotNull(q.Select);
        Assert.Equal(3, q.Select.Count);

        Assert.NotNull(q.ComputedSelect);
        Assert.Single(q.ComputedSelect);
        var c = q.ComputedSelect[0];
        Assert.Equal("price", c.LeftColumn);
        Assert.Equal("quantity", c.RightColumn);
        Assert.Equal("total", c.Alias);
    }

    [Fact]
    public void Computed_AllOperators()
    {
        var ops = new[] { ("+", ArithmeticOp.Add), ("-", ArithmeticOp.Subtract), ("*", ArithmeticOp.Multiply), ("/", ArithmeticOp.Divide) };

        foreach (var (sym, expectedOp) in ops)
        {
            var result = QueryParser.Parse($"get t select a {sym} b as r");
            Assert.True(result.Success, $"Failed for operator {sym}");
            var q = Assert.IsType<GetQuery>(result.Query);
            Assert.NotNull(q.ComputedSelect);
            Assert.Equal(expectedOp, q.ComputedSelect[0].Operator);
        }
    }

    [Fact]
    public void Computed_NegativeLiteral()
    {
        var result = QueryParser.Parse("get orders select amount + -5 as adjusted");

        Assert.True(result.Success);
        var q = Assert.IsType<GetQuery>(result.Query);
        Assert.NotNull(q.ComputedSelect);
        Assert.Equal(-5.0, q.ComputedSelect[0].RightLiteral);
    }

    [Fact]
    public void Computed_MissingAlias_Error()
    {
        var result = QueryParser.Parse("get orders select amount * 0.19");

        Assert.False(result.Success);
    }

    [Fact]
    public void Computed_OnlyComputed_NoSimpleSelect()
    {
        var result = QueryParser.Parse("get orders select price * quantity as total");

        Assert.True(result.Success);
        var q = Assert.IsType<GetQuery>(result.Query);
        // select list has no simple columns, but computed is present
        Assert.NotNull(q.ComputedSelect);
        Assert.Single(q.ComputedSelect);
    }

    [Fact]
    public void Computed_CaseInsensitive()
    {
        var result = QueryParser.Parse("GET Orders SELECT Amount * 0.19 AS Tax");

        Assert.True(result.Success);
        var q = Assert.IsType<GetQuery>(result.Query);
        Assert.NotNull(q.ComputedSelect);
        Assert.Equal("amount", q.ComputedSelect[0].LeftColumn);
        Assert.Equal("tax", q.ComputedSelect[0].Alias);
    }

    // ── DELETE ──────────────────────────────────────────────

    [Fact]
    public void Delete_Simple()
    {
        var result = QueryParser.Parse("delete users where age < 18");

        Assert.True(result.Success);
        var q = Assert.IsType<DeleteQuery>(result.Query);
        Assert.Equal("users", q.Table);
        var c = Assert.IsType<CompareNode>(q.Where);
        Assert.Equal("age", c.Column);
        Assert.Equal(CompareOp.LessThan, c.Operator);
        Assert.Equal("18", c.Value);
    }

    [Fact]
    public void Delete_ById()
    {
        var result = QueryParser.Parse("delete users where id = 5");

        Assert.True(result.Success);
        var q = Assert.IsType<DeleteQuery>(result.Query);
        var c = Assert.IsType<CompareNode>(q.Where);
        Assert.Equal("id", c.Column);
        Assert.Equal(CompareOp.Equal, c.Operator);
        Assert.Equal("5", c.Value);
    }

    [Fact]
    public void Delete_WithoutWhere_Error()
    {
        var result = QueryParser.Parse("delete users");

        Assert.False(result.Success);
        Assert.Contains(result.Errors!, e => e.Code == "WHERE_REQUIRED");
    }

    [Fact]
    public void Delete_CaseInsensitive()
    {
        var result = QueryParser.Parse("DELETE Users WHERE Age > 30");

        Assert.True(result.Success);
        var q = Assert.IsType<DeleteQuery>(result.Query);
        Assert.Equal("users", q.Table);
        var c = Assert.IsType<CompareNode>(q.Where);
        Assert.Equal("age", c.Column);
    }
}

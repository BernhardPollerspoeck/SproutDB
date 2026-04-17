using SproutDB.Core.Parsing;

namespace SproutDB.Core.Tests;

public class QueryFormatterTests
{
    // ── Rule 1: GET clauses on new lines with indentation ──

    [Fact]
    public void Get_Simple_NoChange()
    {
        var result = SproutQueryFormatter.Format("get users");
        Assert.Equal("get users", result);
    }

    [Fact]
    public void Get_Where_OnNewLine()
    {
        var result = SproutQueryFormatter.Format("get users where active = true");
        Assert.Equal("get users\n    where active = true", result);
    }

    [Fact]
    public void Get_MultipleClauses_EachOnNewLine()
    {
        var result = SproutQueryFormatter.Format("get users where active = true order by name asc limit 10");
        Assert.Equal("get users\n    where active = true\n    order by name asc\n    limit 10", result);
    }

    [Fact]
    public void Get_Select_OnNewLine()
    {
        var result = SproutQueryFormatter.Format("get users select name, email where active = true");
        Assert.Equal("get users\n    select name , email\n    where active = true", result);
    }

    [Fact]
    public void Get_GroupBy_StaysTogether()
    {
        var result = SproutQueryFormatter.Format("get orders group by status");
        Assert.Equal("get orders\n    group by status", result);
    }

    [Fact]
    public void Get_OrderBy_StaysTogether()
    {
        var result = SproutQueryFormatter.Format("get users order by name asc");
        Assert.Equal("get users\n    order by name asc", result);
    }

    [Fact]
    public void Get_PageSize_StaysTogether()
    {
        var result = SproutQueryFormatter.Format("get users page 2 size 50");
        Assert.Equal("get users\n    page 2 size 50", result);
    }

    [Fact]
    public void Get_Distinct()
    {
        var result = SproutQueryFormatter.Format("get users select name distinct");
        Assert.Equal("get users\n    select name\n    distinct", result);
    }

    [Fact]
    public void Get_Count()
    {
        var result = SproutQueryFormatter.Format("get users count");
        Assert.Equal("get users\n    count", result);
    }

    [Fact]
    public void Get_Aggregate_Sum()
    {
        var result = SproutQueryFormatter.Format("get orders sum total where status = 'completed'");
        Assert.Equal("get orders\n    sum total\n    where status = 'completed'", result);
    }

    [Fact]
    public void Get_MinusSelect()
    {
        var result = SproutQueryFormatter.Format("get users -select password where active = true");
        Assert.Equal("get users\n    -select password\n    where active = true", result);
    }

    // ── Rule 2: follow on new line, sub-where indented deeper ──

    [Fact]
    public void Get_Follow_OnNewLine()
    {
        var result = SproutQueryFormatter.Format("get users follow users._id -> orders.user_id as orders");
        Assert.Equal("get users\n    follow users._id -> orders.user_id as orders", result);
    }

    [Fact]
    public void Get_Follow_WithSubWhere()
    {
        var result = SproutQueryFormatter.Format("get users follow users._id -> orders.user_id as orders where status = 'completed'");
        Assert.Equal(
            "get users\n    follow users._id -> orders.user_id as orders\n        where status = 'completed'",
            result);
    }

    [Fact]
    public void Get_Where_Then_Follow()
    {
        var result = SproutQueryFormatter.Format("get users where active = true follow users._id -> orders.user_id as orders");
        Assert.Equal(
            "get users\n    where active = true\n    follow users._id -> orders.user_id as orders",
            result);
    }

    // ── Rule 3: and/or on new lines with extra indentation ──

    [Fact]
    public void Get_Where_And_OnNewLine()
    {
        var result = SproutQueryFormatter.Format("get users where name = 'Alice' and age > 30");
        Assert.Equal("get users\n    where name = 'Alice'\n      and age > 30", result);
    }

    [Fact]
    public void Get_Where_Or_OnNewLine()
    {
        var result = SproutQueryFormatter.Format("get users where status = 'active' or status = 'pending'");
        Assert.Equal("get users\n    where status = 'active'\n      or status = 'pending'", result);
    }

    [Fact]
    public void Get_Where_MultipleAndOr()
    {
        var result = SproutQueryFormatter.Format("get users where active = true and age > 18 or name = 'admin'");
        Assert.Equal("get users\n    where active = true\n      and age > 18\n      or name = 'admin'", result);
    }

    [Fact]
    public void Get_Follow_SubWhere_And()
    {
        var result = SproutQueryFormatter.Format("get users follow users._id -> orders.user_id as orders where status = 'completed' and total > 100");
        Assert.Equal(
            "get users\n    follow users._id -> orders.user_id as orders\n        where status = 'completed'\n          and total > 100",
            result);
    }

    // ── Rule 4: Upsert single multi-line, bulk compact ──

    [Fact]
    public void Upsert_Single_OneField_Compact()
    {
        var result = SproutQueryFormatter.Format("upsert users { name: 'Alice' }");
        Assert.Equal("upsert users { name : 'Alice' }", result);
    }

    [Fact]
    public void Upsert_Single_MultiField_MultiLine()
    {
        var result = SproutQueryFormatter.Format("upsert users { name: 'Alice', age: 28, email: 'alice@test.com' }");
        Assert.Equal("upsert users {\n    name: 'Alice',\n    age: 28,\n    email: 'alice@test.com'\n}", result);
    }

    [Fact]
    public void Upsert_Single_WithOn()
    {
        var result = SproutQueryFormatter.Format("upsert users { name: 'Alice', email: 'a@b.com' } on email");
        Assert.Equal("upsert users {\n    name: 'Alice',\n    email: 'a@b.com'\n}\n    on email", result);
    }

    [Fact]
    public void Upsert_Bulk_StaysCompact()
    {
        var result = SproutQueryFormatter.Format("upsert users [{ name: 'Alice' }, { name: 'Bob' }]");
        Assert.Equal("upsert users [ { name : 'Alice' } , { name : 'Bob' } ]", result);
    }

    [Fact]
    public void Upsert_Bulk_WithOn()
    {
        var result = SproutQueryFormatter.Format("upsert users [{ name: 'Alice' }] on name");
        Assert.Equal("upsert users [ { name : 'Alice' } ]\n    on name", result);
    }

    // ── Delete formatting ──

    [Fact]
    public void Delete_Where_OnNewLine()
    {
        var result = SproutQueryFormatter.Format("delete users where name = 'Bob'");
        Assert.Equal("delete users\n    where name = 'Bob'", result);
    }

    [Fact]
    public void Delete_Where_And()
    {
        var result = SproutQueryFormatter.Format("delete users where name = 'Bob' and age > 30");
        Assert.Equal("delete users\n    where name = 'Bob'\n      and age > 30", result);
    }

    // ── Simple commands (no special formatting) ──

    [Fact]
    public void Describe_NoChange()
    {
        var result = SproutQueryFormatter.Format("describe users");
        Assert.Equal("describe users", result);
    }

    [Fact]
    public void Create_Table_NoChange()
    {
        var result = SproutQueryFormatter.Format("create table users");
        Assert.Equal("create table users", result);
    }

    [Fact]
    public void Purge_Table_NoChange()
    {
        var result = SproutQueryFormatter.Format("purge table users");
        Assert.Equal("purge table users", result);
    }

    // ── Edge cases ──

    [Fact]
    public void Empty_Query_ReturnsEmpty()
    {
        Assert.Equal("", SproutQueryFormatter.Format(""));
    }

    [Fact]
    public void Whitespace_Only_ReturnsAsIs()
    {
        Assert.Equal("   ", SproutQueryFormatter.Format("   "));
    }

    [Fact]
    public void Already_Formatted_Idempotent()
    {
        var formatted = "get users\n    where active = true\n      and age > 18\n    order by name asc\n    limit 10";
        var result = SproutQueryFormatter.Format(formatted);
        Assert.Equal(formatted, result);
    }

    [Fact]
    public void Get_FullComplex_Query()
    {
        var input = "get users select name, email where active = true and age > 18 order by name asc limit 50";
        var expected = "get users\n    select name , email\n    where active = true\n      and age > 18\n    order by name asc\n    limit 50";
        Assert.Equal(expected, SproutQueryFormatter.Format(input));
    }

    [Fact]
    public void Get_Follow_WithSelect()
    {
        var result = SproutQueryFormatter.Format("get users follow users._id -> orders.user_id as orders select product, status");
        Assert.Equal(
            "get users\n    follow users._id -> orders.user_id as orders\n        select product , status",
            result);
    }

    [Fact]
    public void Get_Follow_WithSelect_AndWhere()
    {
        var result = SproutQueryFormatter.Format("get users follow users._id -> orders.user_id as orders select product where status = 'completed'");
        Assert.Equal(
            "get users\n    follow users._id -> orders.user_id as orders\n        select product\n        where status = 'completed'",
            result);
    }

    // ── Regression: alias.column in SELECT must not get spaces around the dot ──

    [Fact]
    public void Get_TopLevelSelect_AliasColumn_NoSpaceAroundDot()
    {
        var result = SproutQueryFormatter.Format(
            "get orders follow orders.customer_id -> customers._id as c select c.name, order_date, status");

        Assert.Contains("select c.name", result);
        Assert.DoesNotContain("c . name", result);
    }

    [Fact]
    public void Get_FollowSelect_AliasColumn_NoSpaceAroundDot()
    {
        var result = SproutQueryFormatter.Format(
            "get users follow users._id -> orders.user_id as o select o.total, o.date");

        Assert.Contains("select o.total", result);
        Assert.Contains("o.date", result);
        Assert.DoesNotContain("o . total", result);
        Assert.DoesNotContain("o . date", result);
    }
}

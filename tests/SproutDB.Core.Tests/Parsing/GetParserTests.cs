using SproutDB.Core.Parsing;

namespace SproutDB.Core.Tests.Parsing;

public class GetParserTests
{
    [Fact]
    public void GetAll_Success()
    {
        var result = QueryParser.Parse("get users");

        Assert.True(result.Success);
        var q = Assert.IsType<GetQuery>(result.Query);
        Assert.Equal(SproutOperation.Get, q.Operation);
        Assert.Equal("users", q.Table);
        Assert.Null(q.Select);
    }

    [Fact]
    public void GetAll_CaseInsensitive()
    {
        var result = QueryParser.Parse("GET Users");

        Assert.True(result.Success);
        var q = Assert.IsType<GetQuery>(result.Query);
        Assert.Equal("users", q.Table);
    }

    [Fact]
    public void GetWithSelect_SingleColumn()
    {
        var result = QueryParser.Parse("get users select name");

        Assert.True(result.Success);
        var q = Assert.IsType<GetQuery>(result.Query);
        Assert.NotNull(q.Select);
        Assert.Single(q.Select);
        Assert.Equal("name", q.Select[0].Name);
    }

    [Fact]
    public void GetWithSelect_MultipleColumns()
    {
        var result = QueryParser.Parse("get users select name, email, age");

        Assert.True(result.Success);
        var q = Assert.IsType<GetQuery>(result.Query);
        Assert.NotNull(q.Select);
        Assert.Equal(3, q.Select.Count);
        Assert.Equal("name", q.Select[0].Name);
        Assert.Equal("email", q.Select[1].Name);
        Assert.Equal("age", q.Select[2].Name);
    }

    [Fact]
    public void GetWithSelect_IncludesId()
    {
        var result = QueryParser.Parse("get users select _id, name");

        Assert.True(result.Success);
        var q = Assert.IsType<GetQuery>(result.Query);
        Assert.NotNull(q.Select);
        Assert.Equal(2, q.Select.Count);
        Assert.Equal("_id", q.Select[0].Name);
        Assert.Equal("name", q.Select[1].Name);
    }

    [Fact]
    public void GetWithSelect_ColumnNamesLowercased()
    {
        var result = QueryParser.Parse("get users select Name, EMAIL");

        Assert.True(result.Success);
        var q = Assert.IsType<GetQuery>(result.Query);
        Assert.Equal("name", q.Select![0].Name);
        Assert.Equal("email", q.Select[1].Name);
    }

    [Fact]
    public void GetWithComment_Success()
    {
        var result = QueryParser.Parse("get users ##all users##");

        Assert.True(result.Success);
        var q = Assert.IsType<GetQuery>(result.Query);
        Assert.Equal("users", q.Table);
        Assert.Null(q.Select);
    }

    [Fact]
    public void GetWithSelect_AndComment_Success()
    {
        var result = QueryParser.Parse("get users select name ##just names##");

        Assert.True(result.Success);
        var q = Assert.IsType<GetQuery>(result.Query);
        Assert.Single(q.Select!);
        Assert.Equal("name", q.Select![0].Name);
    }

    // ── Exclude select (-select) ──────────────────────────────

    [Fact]
    public void ExcludeSelect_SingleColumn()
    {
        var result = QueryParser.Parse("get users -select age");

        Assert.True(result.Success);
        var q = Assert.IsType<GetQuery>(result.Query);
        Assert.True(q.ExcludeSelect);
        Assert.NotNull(q.Select);
        Assert.Single(q.Select);
        Assert.Equal("age", q.Select[0].Name);
    }

    [Fact]
    public void ExcludeSelect_MultipleColumns()
    {
        var result = QueryParser.Parse("get users -select age, email");

        Assert.True(result.Success);
        var q = Assert.IsType<GetQuery>(result.Query);
        Assert.True(q.ExcludeSelect);
        Assert.Equal(2, q.Select?.Count);
    }

    [Fact]
    public void ExcludeSelect_WithWhere()
    {
        var result = QueryParser.Parse("get users -select age where name = 'Alice'");

        Assert.True(result.Success);
        var q = Assert.IsType<GetQuery>(result.Query);
        Assert.True(q.ExcludeSelect);
        Assert.NotNull(q.Where);
    }

    [Fact]
    public void RegularSelect_NotExclude()
    {
        var result = QueryParser.Parse("get users select name");

        Assert.True(result.Success);
        var q = Assert.IsType<GetQuery>(result.Query);
        Assert.False(q.ExcludeSelect);
    }

    // ── Error cases ───────────────────────────────────────────

    [Fact]
    public void Get_MissingTableName_Error()
    {
        var result = QueryParser.Parse("get");

        Assert.False(result.Success);
        Assert.Equal("SYNTAX_ERROR", result.Errors![0].Code);
        Assert.Contains("expected table name", result.Errors[0].Message);
    }

    [Fact]
    public void Get_SelectWithoutColumns_Error()
    {
        var result = QueryParser.Parse("get users select");

        Assert.False(result.Success);
        Assert.Equal("SYNTAX_ERROR", result.Errors![0].Code);
        Assert.Contains("expected column name", result.Errors[0].Message);
    }

    [Fact]
    public void Get_TrailingCommaInSelect_Error()
    {
        var result = QueryParser.Parse("get users select name,");

        Assert.False(result.Success);
        Assert.Equal("SYNTAX_ERROR", result.Errors![0].Code);
    }

    [Fact]
    public void Get_ExtraTokensAfterSelect_Error()
    {
        var result = QueryParser.Parse("get users select name extra");

        Assert.False(result.Success);
        Assert.Contains("unexpected token", result.Errors![0].Message);
    }

    [Fact]
    public void OrderByAsc_ThenLimit_Success()
    {
        var result = QueryParser.Parse("get users order by name asc limit 10");

        Assert.True(result.Success);
        var q = Assert.IsType<GetQuery>(result.Query);
        Assert.NotNull(q.OrderBy);
        Assert.Single(q.OrderBy);
        Assert.Equal("name", q.OrderBy[0].Name);
        Assert.False(q.OrderBy[0].Descending);
        Assert.Equal(10, q.Limit);
    }

    [Fact]
    public void OrderByDesc_ThenLimit_Success()
    {
        var result = QueryParser.Parse("get users order by name desc limit 5");

        Assert.True(result.Success);
        var q = Assert.IsType<GetQuery>(result.Query);
        Assert.NotNull(q.OrderBy);
        Assert.True(q.OrderBy[0].Descending);
        Assert.Equal(5, q.Limit);
    }

    [Fact]
    public void OrderByMultiple_AscDesc_ThenLimit()
    {
        var result = QueryParser.Parse("get users order by name asc, age desc limit 20");

        Assert.True(result.Success);
        var q = Assert.IsType<GetQuery>(result.Query);
        Assert.NotNull(q.OrderBy);
        Assert.Equal(2, q.OrderBy.Count);
        Assert.Equal("name", q.OrderBy[0].Name);
        Assert.False(q.OrderBy[0].Descending);
        Assert.Equal("age", q.OrderBy[1].Name);
        Assert.True(q.OrderBy[1].Descending);
        Assert.Equal(20, q.Limit);
    }

    [Fact]
    public void Select_Where_OrderBy_Limit_Success()
    {
        var result = QueryParser.Parse("get users select name, email where active = true order by name asc limit 10");

        Assert.True(result.Success);
        var q = Assert.IsType<GetQuery>(result.Query);
        Assert.NotNull(q.Select);
        Assert.Equal(2, q.Select.Count);
        Assert.NotNull(q.Where);
        Assert.NotNull(q.OrderBy);
        Assert.Equal(10, q.Limit);
    }

    [Fact]
    public void ComplexQuery_Select_Where_Or_And_OrderBy_Limit_Follow()
    {
        var query = "get customers select name, email, city where city = 'Berlin' or city = 'München' and joined_at > '2024-09-01' order by name asc limit 20 follow customers._id -> orders.customer_id as orders where status = 'delivered'";

        var result = QueryParser.Parse(query);

        Assert.True(result.Success);
        var q = Assert.IsType<GetQuery>(result.Query);
        Assert.NotNull(q.Select);
        Assert.Equal(3, q.Select.Count);
        Assert.NotNull(q.Where);
        Assert.NotNull(q.OrderBy);
        Assert.Equal(20, q.Limit);
        Assert.NotNull(q.Follow);
        Assert.Single(q.Follow);
        Assert.Equal("orders", q.Follow[0].Alias);
    }

    // ── Isolating the complex query: building up piece by piece ──

    // Level 1: where with single condition + order by
    [Fact]
    public void Where_Single_Then_OrderBy()
    {
        var result = QueryParser.Parse("get users where active = true order by name asc");
        Assert.True(result.Success, string.Join("; ", result.Errors?.Select(e => e.Message) ?? []));
    }

    // Level 2: where with AND + order by
    [Fact]
    public void Where_And_Then_OrderBy()
    {
        var result = QueryParser.Parse("get users where active = true and age > 18 order by name asc");
        Assert.True(result.Success, string.Join("; ", result.Errors?.Select(e => e.Message) ?? []));
    }

    // Level 3: where with OR + order by
    [Fact]
    public void Where_Or_Then_OrderBy()
    {
        var result = QueryParser.Parse("get users where city = 'Berlin' or city = 'München' order by name asc");
        Assert.True(result.Success, string.Join("; ", result.Errors?.Select(e => e.Message) ?? []));
    }

    // Level 4: where with OR + AND + order by (the exact pattern from the bug)
    [Fact]
    public void Where_Or_And_Then_OrderBy()
    {
        var result = QueryParser.Parse("get users where city = 'München' or city = 'Berlin' and joined_at > '2024-09-01' order by name asc");
        Assert.True(result.Success, string.Join("; ", result.Errors?.Select(e => e.Message) ?? []));
    }

    // Level 5: select + where OR AND + order by
    [Fact]
    public void Select_Where_Or_And_Then_OrderBy()
    {
        var result = QueryParser.Parse("get customers select name, email, city where city = 'München' or city = 'Berlin' and joined_at > '2024-09-01' order by name asc");
        Assert.True(result.Success, string.Join("; ", result.Errors?.Select(e => e.Message) ?? []));
    }

    // Level 6: + limit
    [Fact]
    public void Select_Where_Or_And_OrderBy_Limit()
    {
        var result = QueryParser.Parse("get customers select name, email, city where city = 'München' or city = 'Berlin' and joined_at > '2024-09-01' order by name asc limit 20");
        Assert.True(result.Success, string.Join("; ", result.Errors?.Select(e => e.Message) ?? []));
    }

    // Level 7: + single follow
    [Fact]
    public void Select_Where_Or_And_OrderBy_Limit_Follow()
    {
        var result = QueryParser.Parse("get customers select name, email, city where city = 'München' or city = 'Berlin' and joined_at > '2024-09-01' order by name asc limit 20 follow customers._id -> orders.customer_id as orders");
        Assert.True(result.Success, string.Join("; ", result.Errors?.Select(e => e.Message) ?? []));
    }

    // Level 8: + follow with sub-where
    [Fact]
    public void Select_Where_Or_And_OrderBy_Limit_Follow_SubWhere()
    {
        var result = QueryParser.Parse("get customers select name, email, city where city = 'München' or city = 'Berlin' and joined_at > '2024-09-01' order by name asc limit 20 follow customers._id -> orders.customer_id as orders where status = 'delivered' and total > 50");
        Assert.True(result.Success, string.Join("; ", result.Errors?.Select(e => e.Message) ?? []));
    }

    // Level 9: + second follow (the full query from the bug report)
    [Fact]
    public void FullQuery_Select_Where_OrderBy_Limit_TwoFollows()
    {
        var query = "get customers select name, email, city where city = 'München' or city = 'Berlin' and joined_at > '2024-09-01' order by name asc limit 20 follow customers._id -> orders.customer_id as orders where status = 'delivered' and total > 50 follow orders._id -> order_items.order_id as items";

        var result = QueryParser.Parse(query);

        Assert.True(result.Success, string.Join("; ", result.Errors?.Select(e => e.Message) ?? []));
        var q = Assert.IsType<GetQuery>(result.Query);
        Assert.NotNull(q.Select);
        Assert.Equal(3, q.Select.Count);
        Assert.NotNull(q.Where);
        Assert.NotNull(q.OrderBy);
        Assert.Single(q.OrderBy);
        Assert.Equal("name", q.OrderBy[0].Name);
        Assert.Equal(20, q.Limit);
        Assert.NotNull(q.Follow);
        Assert.Equal(2, q.Follow.Count);
        Assert.Equal("orders", q.Follow[0].Alias);
        Assert.Equal("items", q.Follow[1].Alias);
    }

    // ── Additional isolation: where with string comparison using > (could confuse parser) ──

    [Fact]
    public void Where_StringGreaterThan_Then_OrderBy()
    {
        var result = QueryParser.Parse("get users where joined_at > '2024-09-01' order by name asc");
        Assert.True(result.Success, string.Join("; ", result.Errors?.Select(e => e.Message) ?? []));
    }

    [Fact]
    public void Where_And_StringGreaterThan_Then_OrderBy()
    {
        var result = QueryParser.Parse("get users where active = true and joined_at > '2024-09-01' order by name asc");
        Assert.True(result.Success, string.Join("; ", result.Errors?.Select(e => e.Message) ?? []));
    }

    // Does OR before AND with > cause issues?
    [Fact]
    public void Where_Or_Then_And_GreaterThan_Then_OrderBy()
    {
        var result = QueryParser.Parse("get users where city = 'Berlin' or city = 'München' and joined_at > '2024-09-01' order by name asc");
        Assert.True(result.Success, string.Join("; ", result.Errors?.Select(e => e.Message) ?? []));
    }

    // ── Follow select ────────────────────────────────────────

    [Fact]
    public void Follow_WithSelect_Parsed()
    {
        var result = QueryParser.Parse("get users follow users._id -> orders.user_id as orders select product, status");
        Assert.True(result.Success, string.Join("; ", result.Errors?.Select(e => e.Message) ?? []));
        var q = Assert.IsType<GetQuery>(result.Query);
        Assert.NotNull(q.Follow);
        Assert.Single(q.Follow);
        Assert.NotNull(q.Follow[0].Select);
        Assert.Equal(2, q.Follow[0].Select.Count);
        Assert.Equal("product", q.Follow[0].Select[0].Name);
        Assert.Equal("status", q.Follow[0].Select[1].Name);
    }

    [Fact]
    public void Follow_WithSelect_AndWhere_Parsed()
    {
        var result = QueryParser.Parse("get users follow users._id -> orders.user_id as orders select product where status = 'completed'");
        Assert.True(result.Success, string.Join("; ", result.Errors?.Select(e => e.Message) ?? []));
        var q = Assert.IsType<GetQuery>(result.Query);
        Assert.NotNull(q.Follow);
        Assert.NotNull(q.Follow[0].Select);
        Assert.Single(q.Follow[0].Select);
        Assert.Equal("product", q.Follow[0].Select[0].Name);
        Assert.NotNull(q.Follow[0].Where);
    }

    [Fact]
    public void Follow_WithoutSelect_SelectIsNull()
    {
        var result = QueryParser.Parse("get users follow users._id -> orders.user_id as orders where status = 'completed'");
        Assert.True(result.Success);
        var q = Assert.IsType<GetQuery>(result.Query);
        Assert.Null(q.Follow?[0].Select);
        Assert.NotNull(q.Follow?[0].Where);
    }

    [Fact]
    public void Follow_TwoFollows_EachWithSelect()
    {
        var result = QueryParser.Parse("get users follow users._id -> orders.user_id as orders select product follow orders._id -> items.order_id as items select quantity");
        Assert.True(result.Success, string.Join("; ", result.Errors?.Select(e => e.Message) ?? []));
        var q = Assert.IsType<GetQuery>(result.Query);
        Assert.Equal(2, q.Follow?.Count);
        Assert.Equal("product", q.Follow?[0].Select?[0].Name);
        Assert.Equal("quantity", q.Follow?[1].Select?[0].Name);
    }
}

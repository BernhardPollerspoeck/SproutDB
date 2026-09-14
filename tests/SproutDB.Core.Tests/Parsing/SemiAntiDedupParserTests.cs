using SproutDB.Core.Parsing;

namespace SproutDB.Core.Tests.Parsing;

public class SemiAntiDedupParserTests
{
    private static GetQuery ParseGet(string query)
    {
        var result = QueryParser.Parse(query);
        Assert.True(result.Success, result.Errors?[0].Message);
        return Assert.IsType<GetQuery>(result.Query);
    }

    private static ParseError ParseFailure(string query)
    {
        var result = QueryParser.Parse(query);
        Assert.False(result.Success);
        Assert.NotNull(result.Errors);
        return result.Errors[0];
    }

    // ── Tokenizer ───────────────────────────────────────────

    [Theory]
    [InlineData("a -?> b", nameof(TokenType.ArrowSemi))]
    [InlineData("a -!> b", nameof(TokenType.ArrowAnti))]
    [InlineData("a->?b", nameof(TokenType.ArrowOptRight))]
    [InlineData("a?->b", nameof(TokenType.ArrowOptLeft))]
    [InlineData("a?->?b", nameof(TokenType.ArrowOptBoth))]
    [InlineData("a->b", nameof(TokenType.Arrow))]
    public void Tokenizer_Arrows(string input, string expectedName)
    {
        var expected = Enum.Parse<TokenType>(expectedName);
        var tokens = Tokenizer.Tokenize(input);

        Assert.Equal([TokenType.Identifier, expected, TokenType.Identifier, TokenType.Eof], tokens.Select(t => t.Type));
    }

    [Fact]
    public void Tokenizer_SemiArrowWithoutSpaces()
    {
        var tokens = Tokenizer.Tokenize("users._id-?>orders.user_id");

        Assert.Contains(tokens, t => t.Type == TokenType.ArrowSemi && t.Length == 3);
    }

    [Fact]
    public void Tokenizer_NotEqualAndNegativeNumber_Unaffected()
    {
        var tokens = Tokenizer.Tokenize("a != -1");

        Assert.Equal([TokenType.Identifier, TokenType.NotEqual, TokenType.Minus, TokenType.IntegerLiteral, TokenType.Eof],
            tokens.Select(t => t.Type));
    }

    // ── Semi / anti follow ──────────────────────────────────

    [Theory]
    [InlineData("-?>", nameof(JoinType.Semi))]
    [InlineData("-!>", nameof(JoinType.Anti))]
    public void Follow_FilterArrow_WithoutAlias(string arrow, string expectedName)
    {
        var expected = Enum.Parse<JoinType>(expectedName);
        var q = ParseGet($"get users follow users._id {arrow} orders.user_id");

        Assert.NotNull(q.Follow);
        var follow = Assert.Single(q.Follow);
        Assert.Equal(expected, follow.JoinType);
        Assert.True(follow.IsFilterOnly);
        Assert.Null(follow.Alias);
        Assert.Equal("orders", follow.TargetTable);
        Assert.Equal("user_id", follow.TargetColumn);
    }

    [Fact]
    public void Follow_Semi_WithAliasAndWhere()
    {
        var q = ParseGet("get users follow users._id -?> orders.user_id as o where o.status = 'x'");

        Assert.NotNull(q.Follow);
        var follow = Assert.Single(q.Follow);
        Assert.Equal("o", follow.Alias);
        Assert.NotNull(follow.Where);
    }

    [Fact]
    public void Follow_SemiWithoutAlias_WhereThenNextFollow()
    {
        var q = ParseGet(
            "get users follow users._id -?> orders.user_id where orders.status = 'x' " +
            "follow users._id -!> user_roles.user_id");

        Assert.NotNull(q.Follow);
        Assert.Equal([JoinType.Semi, JoinType.Anti], q.Follow.Select(f => f.JoinType));
        Assert.NotNull(q.Follow[0].Where);
        Assert.Null(q.Follow[1].Where);
    }

    [Fact]
    public void Follow_Semi_PostFollowSelectWithDots_IsNotFollowSelect()
    {
        var q = ParseGet(
            "get users follow users._id -> orders.user_id as o " +
            "follow o._id -?> order_items.order_id select name, o.amount");

        Assert.NotNull(q.Follow);
        Assert.Null(q.Follow[1].Select);
        Assert.NotNull(q.PostFollowSelect);
        Assert.Equal(2, q.PostFollowSelect.Count);
    }

    [Fact]
    public void Follow_Semi_FollowLevelSelect_IsError()
    {
        var err = ParseFailure("get users follow users._id -?> orders.user_id select status");

        Assert.Equal("SYNTAX_ERROR", err.Code);
        Assert.Contains("'select' is not allowed on a '-?>' follow", err.Message);
    }

    [Fact]
    public void Follow_FromFilterFollow_IsError_PositionOnSourceTable()
    {
        const string query = "get users follow users._id -?> orders.user_id as o follow o._id -> items.order_id as i";
        var err = ParseFailure(query);

        Assert.Equal("SYNTAX_ERROR", err.Code);
        Assert.Equal(query.LastIndexOf("o._id", StringComparison.Ordinal), err.Position);
        Assert.Equal(1, err.Length);
    }

    [Fact]
    public void Follow_FromBaseTableAfterFilterFollowOnSameName_IsAllowed()
    {
        // self-referencing semi: the base table name is never shadowed by a filter follow
        var q = ParseGet(
            "get users follow users.manager_id -?> users._id " +
            "follow users._id -> orders.user_id as o");

        Assert.NotNull(q.Follow);
        Assert.Equal(2, q.Follow.Count);
    }

    [Fact]
    public void Follow_UnknownArrow_MessageListsAllArrows()
    {
        var err = ParseFailure("get users follow users._id = orders.user_id as o");

        Assert.Contains("'-?>' or '-!>'", err.Message);
    }

    // ── dedup by ────────────────────────────────────────────

    [Fact]
    public void DedupBy_SingleColumn()
    {
        var q = ParseGet("get orders dedup by user_id");

        Assert.NotNull(q.DedupBy);
        Assert.Equal("user_id", Assert.Single(q.DedupBy).Name);
    }

    [Fact]
    public void DedupBy_MultipleAndDottedColumns()
    {
        var q = ParseGet("get users follow users._id -> orders.user_id as o dedup by _id, o.status");

        Assert.NotNull(q.DedupBy);
        Assert.Equal(["_id", "o.status"], q.DedupBy.Select(c => c.Name));
    }

    [Fact]
    public void DedupBy_DottedColumn_PositionCoversWholeName()
    {
        const string query = "get users follow users._id -> orders.user_id as o dedup by o.status";
        var q = ParseGet(query);

        Assert.NotNull(q.DedupBy);
        var col = Assert.Single(q.DedupBy);
        Assert.Equal(query.IndexOf("o.status", StringComparison.Ordinal), col.Position);
        Assert.Equal("o.status".Length, col.Length);
    }

    [Theory]
    [InlineData("get orders select user_id dedup by user_id order by user_id limit 5")]
    [InlineData("get orders select user_id order by user_id dedup by user_id limit 5")]
    [InlineData("get orders where amount > 1 dedup by user_id count")]
    [InlineData("get orders select user_id, status distinct dedup by user_id")]
    [InlineData("get orders dedup by user_id page 1 size 10")]
    public void DedupBy_CombinesWithOtherTrailingClauses(string query)
    {
        var q = ParseGet(query);

        Assert.NotNull(q.DedupBy);
    }

    [Fact]
    public void DedupBy_EndsSelectList()
    {
        var q = ParseGet("get orders select user_id, status dedup by user_id");

        Assert.NotNull(q.Select);
        Assert.Equal(2, q.Select.Count);
    }

    [Fact]
    public void DedupBy_EndsGroupByList_ThenConflicts()
    {
        var err = ParseFailure("get orders count group by status dedup by status");

        Assert.Contains("'dedup by' cannot be combined with 'group by'", err.Message);
    }
}

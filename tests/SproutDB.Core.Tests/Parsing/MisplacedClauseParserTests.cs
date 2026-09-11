using SproutDB.Core.Parsing;

namespace SproutDB.Core.Tests.Parsing;

/// <summary>
/// Clauses in the wrong place (typically SQL habits) get a message that says how
/// to fix the query instead of the generic "expected end of query".
/// </summary>
public class MisplacedClauseParserTests
{
    private static string ErrorOf(string query)
    {
        var result = QueryParser.Parse(query);
        Assert.False(result.Success);
        var error = Assert.Single(result.Errors ?? []);
        Assert.Equal(ErrorCodes.SYNTAX_ERROR, error.Code);
        Assert.DoesNotContain("expected end of query", error.Message);
        return error.Message;
    }

    [Theory]
    [InlineData("get gs where grain_key in ['a'] select grain_key")]
    [InlineData("get gs order by name select name")]
    [InlineData("get gs limit 5 select name")]
    public void SelectAfterClause_SaysSelectComesFirst(string query)
    {
        Assert.Contains("'select' must come directly after the table name", ErrorOf(query));
    }

    [Fact]
    public void ExcludeSelectAfterWhere_SaysSelectComesFirst()
    {
        Assert.Contains("'-select' must come directly after the table name", ErrorOf("get users where active = true -select password"));
    }

    [Fact]
    public void SecondSelectAfterFollow_IsExplained()
    {
        var message = ErrorOf("get orders follow orders.user_id -> users._id as u select orders.total where orders.total > 5 select u.name");
        Assert.Contains("after 'follow'", message);
    }

    [Fact]
    public void DistinctAfterWhere_IsExplained()
    {
        Assert.Contains("'distinct' must come directly after the select list", ErrorOf("get users select city where active = true distinct"));
    }

    [Fact]
    public void AggregateAfterWhere_IsExplained()
    {
        Assert.Contains("aggregate functions must come directly after the table name", ErrorOf("get orders where paid = true sum total"));
    }

    [Fact]
    public void DuplicateWhere_SuggestsAndOr()
    {
        Assert.Contains("'where' may appear only once", ErrorOf("get users where a = 1 where b = 2"));
    }

    [Theory]
    [InlineData("get users order by a order by b", "'order by' may appear only once")]
    [InlineData("get users limit 1 limit 2", "'limit' may appear only once")]
    public void DuplicateClause_IsExplained(string query, string expected)
    {
        Assert.Contains(expected, ErrorOf(query));
    }

    [Fact]
    public void ErrorPointsAtMisplacedToken()
    {
        const string query = "get gs where k = 'a' select k";
        var result = QueryParser.Parse(query);

        Assert.StartsWith($"get gs where k = 'a' select ##", result.AnnotatedQuery);
    }

    [Fact]
    public void ValidOrders_StillParse()
    {
        Assert.True(QueryParser.Parse("get users select name where active = true order by name limit 5").Success);
        Assert.True(QueryParser.Parse("get users limit 5 where active = true").Success);
        Assert.True(QueryParser.Parse("get orders follow orders.user_id -> users._id as u select orders.total, u.name where orders.total > 5").Success);
    }

    [Fact]
    public void UpsertOnAfterWhen_IsExplained()
    {
        var message = ErrorOf("upsert gs {_id: 1, etag: 'E2'} when etag = 'E1' on k");
        Assert.Contains("'on' must come before 'when'", message);
    }

    [Fact]
    public void UpsertWhenWithoutOn_MentionsOrder()
    {
        var message = ErrorOf("upsert gs {k: 'a', etag: 'E2'} when etag = 'E1' on k");
        Assert.Contains("'on' clause before it", message);
    }
}

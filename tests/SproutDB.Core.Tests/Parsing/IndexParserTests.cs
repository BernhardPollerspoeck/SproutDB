using SproutDB.Core.Parsing;

namespace SproutDB.Core.Tests.Parsing;

public class IndexParserTests
{
    // ── create index ──────────────────────────────────────

    [Fact]
    public void CreateIndex_ParsesTableAndColumn()
    {
        var result = QueryParser.Parse("create index users.email");

        Assert.True(result.Success);
        var q = Assert.IsType<CreateIndexQuery>(result.Query);
        Assert.Equal("users", q.Table);
        Assert.Equal("email", q.Column);
    }

    [Fact]
    public void CreateIndex_CaseInsensitive()
    {
        var result = QueryParser.Parse("CREATE INDEX Users.Email");

        Assert.True(result.Success);
        var q = Assert.IsType<CreateIndexQuery>(result.Query);
        Assert.Equal("users", q.Table);
        Assert.Equal("email", q.Column);
    }

    [Fact]
    public void CreateIndex_MissingDot_Error()
    {
        var result = QueryParser.Parse("create index users email");

        Assert.False(result.Success);
        Assert.Contains("expected '.'", result.Errors![0].Message);
    }

    [Fact]
    public void CreateIndex_IdColumn_Error()
    {
        var result = QueryParser.Parse("create index users._id");

        Assert.False(result.Success);
        Assert.Contains("'_id' is reserved", result.Errors![0].Message);
    }

    [Fact]
    public void CreateIndex_MissingTableName_Error()
    {
        var result = QueryParser.Parse("create index");

        Assert.False(result.Success);
    }

    [Fact]
    public void CreateIndex_ExtraTokens_Error()
    {
        var result = QueryParser.Parse("create index users.email extra");

        Assert.False(result.Success);
        Assert.Contains("expected end of query", result.Errors![0].Message);
    }

    // ── purge index ───────────────────────────────────────

    [Fact]
    public void PurgeIndex_ParsesTableAndColumn()
    {
        var result = QueryParser.Parse("purge index users.email");

        Assert.True(result.Success);
        var q = Assert.IsType<PurgeIndexQuery>(result.Query);
        Assert.Equal("users", q.Table);
        Assert.Equal("email", q.Column);
    }

    [Fact]
    public void PurgeIndex_MissingDot_Error()
    {
        var result = QueryParser.Parse("purge index users email");

        Assert.False(result.Success);
        Assert.Contains("expected '.'", result.Errors![0].Message);
    }

    [Fact]
    public void PurgeIndex_IdColumn_Error()
    {
        var result = QueryParser.Parse("purge index users._id");

        Assert.False(result.Success);
        Assert.Contains("'_id' is reserved", result.Errors![0].Message);
    }
}

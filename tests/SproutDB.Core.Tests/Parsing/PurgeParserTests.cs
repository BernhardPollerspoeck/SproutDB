using SproutDB.Core.Parsing;

namespace SproutDB.Core.Tests.Parsing;

public class PurgeParserTests
{
    // ── purge column ─────────────────────────────────────────

    [Fact]
    public void PurgeColumn_Success()
    {
        var result = QueryParser.Parse("purge column users.email");

        Assert.True(result.Success);
        var q = Assert.IsType<PurgeColumnQuery>(result.Query);
        Assert.Equal("users", q.Table);
        Assert.Equal("email", q.Column);
    }

    [Fact]
    public void PurgeColumn_CaseInsensitive()
    {
        var result = QueryParser.Parse("PURGE COLUMN Users.Email");

        Assert.True(result.Success);
        var q = Assert.IsType<PurgeColumnQuery>(result.Query);
        Assert.Equal("users", q.Table);
        Assert.Equal("email", q.Column);
    }

    [Fact]
    public void PurgeColumn_IdReserved_Error()
    {
        var result = QueryParser.Parse("purge column users.id");

        Assert.False(result.Success);
        Assert.Contains("'id' is reserved", result.Errors![0].Message);
    }

    [Fact]
    public void PurgeColumn_MissingDot_Error()
    {
        var result = QueryParser.Parse("purge column users email");

        Assert.False(result.Success);
        Assert.Contains("expected '.'", result.Errors![0].Message);
    }

    [Fact]
    public void PurgeColumn_MissingColumnName_Error()
    {
        var result = QueryParser.Parse("purge column users.");

        Assert.False(result.Success);
    }

    [Fact]
    public void PurgeColumn_MissingTableName_Error()
    {
        var result = QueryParser.Parse("purge column");

        Assert.False(result.Success);
    }

    [Fact]
    public void PurgeColumn_ExtraTokens_Error()
    {
        var result = QueryParser.Parse("purge column users.email extra");

        Assert.False(result.Success);
        Assert.Contains("expected end of query", result.Errors![0].Message);
    }

    // ── purge table ──────────────────────────────────────────

    [Fact]
    public void PurgeTable_Success()
    {
        var result = QueryParser.Parse("purge table users");

        Assert.True(result.Success);
        var q = Assert.IsType<PurgeTableQuery>(result.Query);
        Assert.Equal("users", q.Table);
    }

    [Fact]
    public void PurgeTable_CaseInsensitive()
    {
        var result = QueryParser.Parse("PURGE TABLE Users");

        Assert.True(result.Success);
        var q = Assert.IsType<PurgeTableQuery>(result.Query);
        Assert.Equal("users", q.Table);
    }

    [Fact]
    public void PurgeTable_MissingName_Error()
    {
        var result = QueryParser.Parse("purge table");

        Assert.False(result.Success);
    }

    [Fact]
    public void PurgeTable_ExtraTokens_Error()
    {
        var result = QueryParser.Parse("purge table users extra");

        Assert.False(result.Success);
        Assert.Contains("expected end of query", result.Errors![0].Message);
    }

    // ── purge database ───────────────────────────────────────

    [Fact]
    public void PurgeDatabase_Success()
    {
        var result = QueryParser.Parse("purge database");

        Assert.True(result.Success);
        Assert.IsType<PurgeDatabaseQuery>(result.Query);
    }

    [Fact]
    public void PurgeDatabase_CaseInsensitive()
    {
        var result = QueryParser.Parse("PURGE DATABASE");

        Assert.True(result.Success);
        Assert.IsType<PurgeDatabaseQuery>(result.Query);
    }

    [Fact]
    public void PurgeDatabase_ExtraTokens_Error()
    {
        var result = QueryParser.Parse("purge database extra");

        Assert.False(result.Success);
        Assert.Contains("expected end of query", result.Errors![0].Message);
    }

    // ── purge (no target) ────────────────────────────────────

    [Fact]
    public void Purge_NoTarget_Error()
    {
        var result = QueryParser.Parse("purge");

        Assert.False(result.Success);
        Assert.Contains("expected 'column', 'table' or 'database'", result.Errors![0].Message);
    }

    [Fact]
    public void Purge_UnknownTarget_Error()
    {
        var result = QueryParser.Parse("purge something");

        Assert.False(result.Success);
        Assert.Contains("expected 'column', 'table' or 'database'", result.Errors![0].Message);
    }
}

namespace SproutDB.Core.Tests;

public class PagingTests : IDisposable
{
    private readonly string _tempDir;
    private readonly SproutEngine _engine;

    public PagingTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), $"sproutdb-test-{Guid.NewGuid()}");

        // Small page size for testing
        _engine = new SproutEngine(new SproutEngineSettings
        {
            DataDirectory = _tempDir,
            DefaultPageSize = 3,
        });

        _engine.ExecuteOne("create database", "testdb");
        _engine.ExecuteOne("create table users (name string 100, age ubyte)", "testdb");

        // Seed 10 users
        for (var i = 1; i <= 10; i++)
            _engine.ExecuteOne($"upsert users {{name: 'User{i:D2}', age: {20 + i}}}", "testdb");
    }

    public void Dispose()
    {
        _engine.Dispose();
        if (Directory.Exists(_tempDir))
            Directory.Delete(_tempDir, true);
    }

    // ── Auto-Paging ─────────────────────────────────────────

    [Fact]
    public void AutoPaging_ExceedsPageSize_ReturnsFirstPage()
    {
        var r = _engine.ExecuteOne("get users", "testdb");

        Assert.Equal(SproutOperation.Get, r.Operation);
        Assert.Equal(3, r.Data!.Count);
        Assert.Equal(3, r.Affected);
        Assert.NotNull(r.Paging);
        Assert.Equal(10, r.Paging.Total);
        Assert.Equal(3, r.Paging.PageSize);
        Assert.Equal(1, r.Paging.Page);
        Assert.NotNull(r.Paging.Next);
    }

    [Fact]
    public void AutoPaging_NextQuery_ContainsPageAndSize()
    {
        var r = _engine.ExecuteOne("get users", "testdb");

        Assert.NotNull(r.Paging?.Next);
        Assert.Contains("page 2", r.Paging.Next);
        Assert.Contains("size 3", r.Paging.Next);
    }

    [Fact]
    public void AutoPaging_WithinPageSize_NoPaging()
    {
        // Only 2 matching rows, page size is 3
        var r = _engine.ExecuteOne("get users where age > 29", "testdb");

        Assert.Equal(1, r.Data!.Count);
        Assert.Null(r.Paging);
    }

    [Fact]
    public void AutoPaging_ExactPageSize_NoPaging()
    {
        // Exactly 3 matching rows
        var r = _engine.ExecuteOne("get users where age <= 23", "testdb");

        Assert.Equal(3, r.Data!.Count);
        Assert.Null(r.Paging);
    }

    // ── Manual Paging ───────────────────────────────────────

    [Fact]
    public void ManualPaging_Page1()
    {
        var r = _engine.ExecuteOne("get users page 1 size 3", "testdb");

        Assert.Equal(3, r.Data!.Count);
        Assert.NotNull(r.Paging);
        Assert.Equal(10, r.Paging.Total);
        Assert.Equal(1, r.Paging.Page);
        Assert.Equal(3, r.Paging.PageSize);
        Assert.NotNull(r.Paging.Next);
    }

    [Fact]
    public void ManualPaging_Page2()
    {
        var r = _engine.ExecuteOne("get users page 2 size 3", "testdb");

        Assert.Equal(3, r.Data!.Count);
        Assert.NotNull(r.Paging);
        Assert.Equal(2, r.Paging.Page);
        Assert.NotNull(r.Paging.Next);
    }

    [Fact]
    public void ManualPaging_LastPage()
    {
        // 10 rows, page size 3 → 4 pages (3+3+3+1)
        var r = _engine.ExecuteOne("get users page 4 size 3", "testdb");

        Assert.Single(r.Data!);
        Assert.NotNull(r.Paging);
        Assert.Equal(4, r.Paging.Page);
        Assert.Null(r.Paging.Next); // no more pages
    }

    [Fact]
    public void ManualPaging_BeyondLastPage()
    {
        var r = _engine.ExecuteOne("get users page 5 size 3", "testdb");

        Assert.Empty(r.Data!);
        Assert.NotNull(r.Paging);
        Assert.Equal(5, r.Paging.Page);
        Assert.Null(r.Paging.Next);
    }

    [Fact]
    public void ManualPaging_SizeCappedToDefault()
    {
        // Server default is 3, requesting size 100 → capped to 3
        var r = _engine.ExecuteOne("get users page 1 size 100", "testdb");

        Assert.Equal(3, r.Data!.Count);
        Assert.NotNull(r.Paging);
        Assert.Equal(3, r.Paging.PageSize);
    }

    [Fact]
    public void ManualPaging_WithOrderBy()
    {
        var r = _engine.ExecuteOne("get users order by name desc page 1 size 3", "testdb");

        Assert.Equal(3, r.Data!.Count);
        // Descending: User10, User09, User08
        Assert.Equal("User10", r.Data[0]["name"]);
        Assert.Equal("User09", r.Data[1]["name"]);
        Assert.Equal("User08", r.Data[2]["name"]);
    }

    [Fact]
    public void ManualPaging_WithWhere()
    {
        // age > 25 → User06..User10 = 5 rows
        var r = _engine.ExecuteOne("get users where age > 25 page 1 size 3", "testdb");

        Assert.Equal(3, r.Data!.Count);
        Assert.NotNull(r.Paging);
        Assert.Equal(5, r.Paging.Total);
    }

    [Fact]
    public void ManualPaging_NextQueryReplacesPage()
    {
        var r = _engine.ExecuteOne("get users page 1 size 3", "testdb");

        Assert.NotNull(r.Paging?.Next);
        Assert.Contains("page 2", r.Paging.Next);
        Assert.Contains("size 3", r.Paging.Next);
        // Should not contain "page 1" anymore
        Assert.DoesNotContain("page 1", r.Paging.Next);
    }

    // ── Limit disables auto-paging ──────────────────────────

    [Fact]
    public void Limit_DisablesAutoPaging()
    {
        // limit 5 exceeds page size 3, but should NOT auto-page
        var r = _engine.ExecuteOne("get users limit 5", "testdb");

        Assert.Equal(5, r.Data!.Count);
        Assert.Null(r.Paging);
    }

    // ── Count ignores paging ────────────────────────────────

    [Fact]
    public void Count_NoPaging()
    {
        var r = _engine.ExecuteOne("get users count", "testdb");

        Assert.Equal(10, r.Affected);
        Assert.Empty(r.Data!);
        Assert.Null(r.Paging);
    }
}

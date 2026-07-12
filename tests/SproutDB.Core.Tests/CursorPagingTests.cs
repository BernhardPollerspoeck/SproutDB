namespace SproutDB.Core.Tests;

/// <summary>
/// Keyset paging via <c>after 'CURSOR'</c> and the top-N fast path for
/// <c>order by _id limit N</c>. Both must stay correct when slot order
/// diverges from _id order (deletes + backfill reuse of freed slots).
/// </summary>
public class CursorPagingTests : IDisposable
{
    private readonly string _tempDir;
    private readonly SproutEngine _engine;

    public CursorPagingTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), $"sproutdb-test-{Guid.NewGuid()}");

        _engine = new SproutEngine(new SproutEngineSettings
        {
            DataDirectory = _tempDir,
            DefaultPageSize = 5,
        });

        _engine.ExecuteOne("create database", "testdb");
        _engine.ExecuteOne("create table users (name string 100, age ubyte)", "testdb");

        // Seed 10 users → _id 1..10
        for (var i = 1; i <= 10; i++)
            _engine.ExecuteOne($"upsert users {{name: 'User{i:D2}', age: {20 + i}}}", "testdb");
    }

    public void Dispose()
    {
        _engine.Dispose();
        if (Directory.Exists(_tempDir))
            Directory.Delete(_tempDir, true);
    }

    private static ulong GetId(Dictionary<string, object?> row)
    {
        var value = row["_id"];
        Assert.NotNull(value);
        return Convert.ToUInt64(value);
    }

    // ── Basic cursor pages ───────────────────────────────────

    [Fact]
    public void Cursor_FirstPage_ReturnsRowsAndNextCursor()
    {
        var r = _engine.ExecuteOne("get users after '0' limit 4", "testdb");

        Assert.NotNull(r.Data);
        Assert.Equal(4, r.Data.Count);
        Assert.Equal(1UL, GetId(r.Data[0]));
        Assert.Equal(4UL, GetId(r.Data[3]));

        Assert.NotNull(r.Paging);
        Assert.Equal("4", r.Paging.NextCursor);
        Assert.Equal(10, r.Paging.Total);
        Assert.Equal(4, r.Paging.PageSize);
        Assert.Equal(0, r.Paging.Page);
        Assert.NotNull(r.Paging.Next);
        Assert.Contains("after '4'", r.Paging.Next);
    }

    [Fact]
    public void Cursor_MiddlePage_ContinuesFromCursor()
    {
        var r = _engine.ExecuteOne("get users after '4' limit 4", "testdb");

        Assert.NotNull(r.Data);
        Assert.Equal(4, r.Data.Count);
        Assert.Equal(5UL, GetId(r.Data[0]));
        Assert.Equal(8UL, GetId(r.Data[3]));

        Assert.NotNull(r.Paging);
        Assert.Equal("8", r.Paging.NextCursor);
        Assert.Equal(6, r.Paging.Total); // rows remaining from cursor position
    }

    [Fact]
    public void Cursor_LastPage_NextCursorIsNull()
    {
        var r = _engine.ExecuteOne("get users after '8' limit 4", "testdb");

        Assert.NotNull(r.Data);
        Assert.Equal(2, r.Data.Count);
        Assert.NotNull(r.Paging);
        Assert.Null(r.Paging.NextCursor);
        Assert.Null(r.Paging.Next);
    }

    [Fact]
    public void Cursor_ExactPageBoundary_LastFullPageHasNoNextCursor()
    {
        // 10 rows, pages of 5 → second page is exactly full and final
        var r = _engine.ExecuteOne("get users after '5' limit 5", "testdb");

        Assert.NotNull(r.Data);
        Assert.Equal(5, r.Data.Count);
        Assert.NotNull(r.Paging);
        Assert.Null(r.Paging.NextCursor);
        Assert.Null(r.Paging.Next);
    }

    [Fact]
    public void Cursor_BeyondEnd_ReturnsEmptyPage()
    {
        var r = _engine.ExecuteOne("get users after '999' limit 4", "testdb");

        Assert.NotNull(r.Data);
        Assert.Empty(r.Data);
        Assert.NotNull(r.Paging);
        Assert.Null(r.Paging.NextCursor);
        Assert.Equal(0, r.Paging.Total);
    }

    [Fact]
    public void Cursor_WithoutLimit_UsesDefaultPageSize()
    {
        var r = _engine.ExecuteOne("get users after '0'", "testdb");

        Assert.NotNull(r.Data);
        Assert.Equal(5, r.Data.Count); // DefaultPageSize = 5
        Assert.NotNull(r.Paging);
        Assert.Equal("5", r.Paging.NextCursor);
    }

    [Fact]
    public void Cursor_WithWhere_FiltersAndCounts()
    {
        // age > 25 → User06..User10 = ids 6..10
        var r = _engine.ExecuteOne("get users where age > 25 after '0' limit 3", "testdb");

        Assert.NotNull(r.Data);
        Assert.Equal(3, r.Data.Count);
        Assert.Equal(6UL, GetId(r.Data[0]));
        Assert.NotNull(r.Paging);
        Assert.Equal(5, r.Paging.Total);
        Assert.Equal("8", r.Paging.NextCursor);
    }

    [Fact]
    public void Cursor_WithSelect_ProjectsColumns()
    {
        var r = _engine.ExecuteOne("get users select _id, name after '0' limit 2", "testdb");

        Assert.NotNull(r.Data);
        Assert.Equal(2, r.Data.Count);
        Assert.Equal(2, r.Data[0].Count); // _id + name only
        Assert.True(r.Data[0].ContainsKey("name"));
    }

    [Fact]
    public void Cursor_WithOrderByIdAsc_IsAllowed()
    {
        var r = _engine.ExecuteOne("get users order by _id after '0' limit 3", "testdb");

        Assert.Null(r.Errors);
        Assert.NotNull(r.Data);
        Assert.Equal(3, r.Data.Count);
    }

    [Fact]
    public void Cursor_FollowingNextQuery_WalksAllRowsExactlyOnce()
    {
        var seen = new List<ulong>();
        var response = _engine.ExecuteOne("get users after '0' limit 3", "testdb");

        while (true)
        {
            Assert.NotNull(response.Data);
            foreach (var row in response.Data)
                seen.Add(GetId(row));

            Assert.NotNull(response.Paging);
            if (response.Paging.Next is null)
                break;

            response = _engine.ExecuteOne(response.Paging.Next, "testdb");
        }

        Assert.Equal([1UL, 2, 3, 4, 5, 6, 7, 8, 9, 10], seen);
    }

    // ── Correctness under writes between pages ───────────────

    [Fact]
    public void Cursor_WritesBetweenPages_NoSkipsNoDuplicates()
    {
        var seen = new List<ulong>();

        // Page 1: ids 1..5
        var r1 = _engine.ExecuteOne("get users after '0' limit 5", "testdb");
        Assert.NotNull(r1.Data);
        foreach (var row in r1.Data)
            seen.Add(GetId(row));

        // Between pages: delete an unread row and insert a new one
        _engine.ExecuteOne("delete users where _id = 7", "testdb");
        _engine.ExecuteOne("upsert users {name: 'UserNew', age: 99}", "testdb"); // _id 11

        // Continue from cursor
        Assert.NotNull(r1.Paging?.NextCursor);
        var cursor = r1.Paging.NextCursor;
        while (cursor is not null)
        {
            var r = _engine.ExecuteOne($"get users after '{cursor}' limit 5", "testdb");
            Assert.NotNull(r.Data);
            foreach (var row in r.Data)
                seen.Add(GetId(row));
            Assert.NotNull(r.Paging);
            cursor = r.Paging.NextCursor;
        }

        // Deleted unread row is absent, new row appears at the end,
        // nothing skipped, nothing duplicated.
        Assert.Equal([1UL, 2, 3, 4, 5, 6, 8, 9, 10, 11], seen);
    }

    // ── Slot order ≠ _id order (deletes + backfill) ──────────

    /// <summary>
    /// Fills a table to slot capacity, deletes enough rows to trigger backfill
    /// (freed slots at the front get reused), then inserts new rows. After this,
    /// slot iteration order no longer matches _id order.
    /// Returns the set of surviving ids.
    /// </summary>
    private SortedSet<ulong> BuildBackfilledTable()
    {
        _engine.ExecuteOne("create table items (label string 50) with chunk_size 100", "testdb");

        for (var i = 1; i <= 100; i++)
            _engine.ExecuteOne($"upsert items {{label: 'Item{i:D3}'}}", "testdb");

        var expected = new SortedSet<ulong>();
        for (var i = 1UL; i <= 100; i++)
            expected.Add(i);

        // Delete every 3rd row → 33 free slots (≥ 20% backfill threshold)
        for (var i = 3UL; i <= 99; i += 3)
        {
            _engine.ExecuteOne($"delete items where _id = {i}", "testdb");
            expected.Remove(i);
        }

        // Insert 20 new rows → backfilled into freed low slots with high ids
        for (var i = 1; i <= 20; i++)
        {
            _engine.ExecuteOne($"upsert items {{label: 'New{i:D3}'}}", "testdb");
            expected.Add(100UL + (ulong)i);
        }

        return expected;
    }

    [Fact]
    public void Cursor_AfterBackfill_WalksInIdOrderWithoutLoss()
    {
        var expected = BuildBackfilledTable();

        var seen = new List<ulong>();
        var cursor = "0";
        while (cursor is not null)
        {
            var r = _engine.ExecuteOne($"get items after '{cursor}' limit 7", "testdb");
            Assert.NotNull(r.Data);
            foreach (var row in r.Data)
                seen.Add(GetId(row));
            Assert.NotNull(r.Paging);
            cursor = r.Paging.NextCursor;
        }

        // Strictly ascending — despite slot order being scrambled by backfill
        for (var i = 1; i < seen.Count; i++)
            Assert.True(seen[i - 1] < seen[i], $"ids not ascending at index {i}: {seen[i - 1]} >= {seen[i]}");

        Assert.Equal(expected, new SortedSet<ulong>(seen));
    }

    [Fact]
    public void TopNFastPath_AfterBackfill_MatchesSlowPathSlice()
    {
        var expected = BuildBackfilledTable();

        // Fast path: order by _id limit N
        var fast = _engine.ExecuteOne("get items order by _id limit 10", "testdb");
        Assert.NotNull(fast.Data);
        Assert.Equal(10, fast.Data.Count);

        // Reference: first 10 surviving ids in ascending order
        var expectedFirst10 = expected.Take(10).ToList();
        var fastIds = fast.Data.Select(GetId).ToList();
        Assert.Equal(expectedFirst10, fastIds);
    }

    [Fact]
    public void TopNFastPath_Descending_ReturnsHighestIds()
    {
        var expected = BuildBackfilledTable();

        var r = _engine.ExecuteOne("get items order by _id desc limit 5", "testdb");
        Assert.NotNull(r.Data);
        Assert.Equal(5, r.Data.Count);

        var expectedTop5 = expected.Reverse().Take(5).ToList();
        Assert.Equal(expectedTop5, r.Data.Select(GetId).ToList());
    }

    [Fact]
    public void TopNFastPath_WithWhere_MatchesFilter()
    {
        // age > 25 → ids 6..10; smallest 2 are 6, 7
        var r = _engine.ExecuteOne("get users where age > 25 order by _id limit 2", "testdb");

        Assert.NotNull(r.Data);
        Assert.Equal([6UL, 7UL], r.Data.Select(GetId).ToList());
        Assert.Null(r.Paging); // limit still disables paging
    }

    [Fact]
    public void TopNFastPath_LimitLargerThanTable_ReturnsAllRows()
    {
        var r = _engine.ExecuteOne("get users order by _id limit 50", "testdb");

        Assert.NotNull(r.Data);
        Assert.Equal(10, r.Data.Count);
        Assert.Equal(1UL, GetId(r.Data[0]));
        Assert.Equal(10UL, GetId(r.Data[9]));
    }

    // ── Rejected combinations ────────────────────────────────

    [Theory]
    [InlineData("get users after '0' page 1 size 3", "'page'")]
    [InlineData("get users count after '0'", "'count'")]
    [InlineData("get users after '0' group by age", "'group by'")]
    [InlineData("get users select name distinct after '0'", "'distinct'")]
    [InlineData("get users after '0' follow users._id -> users._id as u", "'follow'")]
    [InlineData("get users sum age after '0'", "aggregate")]
    [InlineData("get users order by name after '0'", "order by _id")]
    [InlineData("get users order by _id desc after '0'", "order by _id")]
    public void Cursor_InvalidCombination_IsSyntaxError(string query, string messagePart)
    {
        var r = _engine.ExecuteOne(query, "testdb");

        Assert.NotNull(r.Errors);
        Assert.Equal("SYNTAX_ERROR", r.Errors[0].Code);
        Assert.Contains(messagePart, r.Errors[0].Message);
    }

    [Fact]
    public void Cursor_NonNumericCursor_IsSyntaxError()
    {
        var r = _engine.ExecuteOne("get users after 'abc' limit 3", "testdb");

        Assert.NotNull(r.Errors);
        Assert.Equal("SYNTAX_ERROR", r.Errors[0].Code);
        Assert.Contains("invalid cursor", r.Errors[0].Message);
    }

    [Fact]
    public void Cursor_MissingStringLiteral_IsSyntaxError()
    {
        var r = _engine.ExecuteOne("get users after 5 limit 3", "testdb");

        Assert.NotNull(r.Errors);
        Assert.Equal("SYNTAX_ERROR", r.Errors[0].Code);
    }

    // ── Existing behavior unchanged ──────────────────────────

    [Fact]
    public void OffsetPaging_StillWorksUnchanged()
    {
        var r = _engine.ExecuteOne("get users page 2 size 3", "testdb");

        Assert.NotNull(r.Data);
        Assert.Equal(3, r.Data.Count);
        Assert.NotNull(r.Paging);
        Assert.Equal(2, r.Paging.Page);
        Assert.Null(r.Paging.NextCursor); // cursor never set for offset paging
    }

    [Fact]
    public void PlainLimit_StillNoPagingInfo()
    {
        var r = _engine.ExecuteOne("get users limit 8", "testdb");

        Assert.NotNull(r.Data);
        Assert.Equal(8, r.Data.Count);
        Assert.Null(r.Paging);
    }
}

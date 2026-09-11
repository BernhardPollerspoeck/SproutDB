namespace SproutDB.Core.Tests;

/// <summary>
/// Conditional writes: <c>upsert … when …</c> and <c>delete … expect N</c>.
/// </summary>
public class ConditionalWriteTests : IDisposable
{
    private class GrainState : ISproutEntity
    {
        public ulong Id { get; set; }
        public string K { get; set; } = "";
        public string? Etag { get; set; }
        public string? Payload { get; set; }
    }

    private readonly string _tempDir;
    private readonly SproutEngine _engine;

    public ConditionalWriteTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), $"sproutdb-test-{Guid.NewGuid()}");
        _engine = new SproutEngine(_tempDir);
        _engine.ExecuteOne("create database", "testdb");
        _engine.ExecuteOne("create table gs (k string 64, etag string 16, payload string 100)", "testdb");
        _engine.ExecuteOne("create index unique gs.k", "testdb");
    }

    public void Dispose()
    {
        _engine.Dispose();
        if (Directory.Exists(_tempDir))
            Directory.Delete(_tempDir, true);
    }

    private SproutResponse Run(string query) => _engine.ExecuteOne(query, "testdb");

    private Dictionary<string, object?>? Row(string key)
    {
        var r = Run($"get gs where k = '{key}'");
        return r.Data?.SingleOrDefault();
    }

    private int RowCount() => Run("get gs").Data?.Count ?? 0;

    private static void AssertConditionFailed(SproutResponse r)
    {
        Assert.Equal(SproutOperation.Error, r.Operation);
        Assert.Equal("CONDITION_FAILED", r.Errors?[0].Code);
    }

    // ── when <expression> ────────────────────────────────────

    [Fact]
    public void When_ConditionHolds_UpdatesExactlyTheFoundRow()
    {
        Run("upsert gs {k: 'a', etag: 'E1', payload: 'x'}");
        Run("upsert gs {k: 'b', etag: 'E1', payload: 'y'}");

        var r = Run("upsert gs {k: 'a', etag: 'E2', payload: 'x2'} on k when etag = 'E1'");

        Assert.Null(r.Errors);
        Assert.Equal(1, r.Affected);
        Assert.Equal((ulong)1, r.Data?[0]["_id"]);
        Assert.Equal("E2", Row("a")?["etag"]);
        Assert.Equal("x2", Row("a")?["payload"]);
        Assert.Equal("E1", Row("b")?["etag"]);
        Assert.Equal(2, RowCount());
    }

    [Fact]
    public void When_ConditionFalse_FailsWithCurrentRow_AndChangesNothing()
    {
        Run("upsert gs {k: 'a', etag: 'E5', payload: 'x'}");

        var r = Run("upsert gs {k: 'a', etag: 'E2', payload: 'new'} on k when etag = 'E1'");

        AssertConditionFailed(r);
        var current = Assert.Single(r.Data ?? []);
        Assert.Equal((ulong)1, current["_id"]);
        Assert.Equal("E5", current["etag"]);
        Assert.Equal("x", current["payload"]);

        Assert.Equal("E5", Row("a")?["etag"]);
        Assert.Equal("x", Row("a")?["payload"]);
    }

    [Fact]
    public void When_RowMissing_FailsAndInsertsNothing()
    {
        var r = Run("upsert gs {k: 'a', etag: 'E2'} on k when etag = 'E1'");

        AssertConditionFailed(r);
        Assert.Empty(r.Data ?? [new Dictionary<string, object?>()]);
        Assert.Equal(0, RowCount());
    }

    [Fact]
    public void When_ErrorIsAnnotatedAtWhenKeyword()
    {
        Run("upsert gs {k: 'a', etag: 'E5'}");
        const string query = "upsert gs {k: 'a', etag: 'E2'} on k when etag = 'E1'";

        var r = Run(query);

        Assert.Equal(query.IndexOf("when", StringComparison.Ordinal), r.Errors?[0].Position);
        Assert.Contains("##condition failed", r.AnnotatedQuery ?? "");
    }

    [Theory]
    [InlineData("etag = 'E1' and payload is null", true)]
    [InlineData("etag = 'E1' and payload is not null", false)]
    [InlineData("etag in ['E0', 'E1']", true)]
    [InlineData("etag != 'E1' or payload is null", true)]
    [InlineData("not etag = 'E1'", false)]
    public void When_SupportsFullWhereGrammar(string condition, bool expectWrite)
    {
        Run("upsert gs {k: 'a', etag: 'E1'}");

        var r = Run($"upsert gs {{k: 'a', etag: 'E2'}} on k when {condition}");

        if (expectWrite)
        {
            Assert.Null(r.Errors);
            Assert.Equal("E2", Row("a")?["etag"]);
        }
        else
        {
            AssertConditionFailed(r);
            Assert.Equal("E1", Row("a")?["etag"]);
        }
    }

    [Fact]
    public void When_WithImplicitId_UsesRowById()
    {
        Run("upsert gs {k: 'a', etag: 'E1'}");

        var ok = Run("upsert gs {_id: 1, etag: 'E2'} when etag = 'E1'");
        Assert.Null(ok.Errors);
        Assert.Equal("E2", Row("a")?["etag"]);

        var stale = Run("upsert gs {_id: 1, etag: 'E3'} when etag = 'E1'");
        AssertConditionFailed(stale);
    }

    [Fact]
    public void When_WithImplicitId_MissingRow_IsConditionFailed_NotIdNotFound()
    {
        var r = Run("upsert gs {_id: 42, etag: 'E2'} when etag = 'E1'");
        AssertConditionFailed(r);
    }

    [Fact]
    public void When_UnknownColumnInCondition_ReportsUnknownColumn()
    {
        Run("upsert gs {k: 'a', etag: 'E1'}");

        var r = Run("upsert gs {k: 'a', etag: 'E2'} on k when nope = 'E1'");

        Assert.Equal("UNKNOWN_COLUMN", r.Errors?[0].Code);
    }

    // ── when exists / when not exists ────────────────────────

    [Fact]
    public void WhenNotExists_Missing_Inserts()
    {
        var r = Run("upsert gs {k: 'a', etag: 'E1'} on k when not exists");

        Assert.Null(r.Errors);
        Assert.Equal("E1", Row("a")?["etag"]);
    }

    [Fact]
    public void WhenNotExists_Existing_FailsWithExistingRow()
    {
        Run("upsert gs {k: 'a', etag: 'E1', payload: 'keep'}");

        var r = Run("upsert gs {k: 'a', etag: 'X'} on k when not exists");

        AssertConditionFailed(r);
        var current = Assert.Single(r.Data ?? []);
        Assert.Equal("E1", current["etag"]);
        Assert.Equal("keep", Row("a")?["payload"]);
    }

    [Fact]
    public void WhenExists_Existing_Updates()
    {
        Run("upsert gs {k: 'a', etag: 'E1'}");

        var r = Run("upsert gs {k: 'a', etag: 'E2'} on k when exists");

        Assert.Null(r.Errors);
        Assert.Equal("E2", Row("a")?["etag"]);
    }

    [Fact]
    public void WhenExists_Missing_FailsAndInsertsNothing()
    {
        var r = Run("upsert gs {k: 'a', etag: 'E2'} on k when exists");

        AssertConditionFailed(r);
        Assert.Equal(0, RowCount());
    }

    [Fact]
    public void ColumnNamedExists_IsUsableInCondition()
    {
        Run("create table flags (k string 10, exists bool)");
        Run("upsert flags {k: 'a', exists: true}");

        var r = Run("upsert flags {k: 'a', exists: false} on k when exists = true");

        Assert.Null(r.Errors);
    }

    // ── TTL: expired rows count as not existing ──────────────

    [Fact]
    public void WhenNotExists_ExpiredRow_IsFreedAndReplacedByNewRow()
    {
        Run("upsert gs {k: 'a', etag: 'E1', payload: 'old', ttl: 1s}");
        Thread.Sleep(1100);

        var r = Run("upsert gs {k: 'a', etag: 'N1'} on k when not exists");

        Assert.Null(r.Errors);
        Assert.Equal((ulong)2, r.Data?[0]["_id"]); // genuinely new row

        var row = Row("a");
        Assert.NotNull(row);
        Assert.Equal((ulong)2, row["_id"]);
        Assert.Equal("N1", row["etag"]);
        Assert.Null(row["payload"]); // nothing carried over from the expired row

        // The expired row is physically gone, not just hidden: a plain 'on k' lookup
        // scans slots in order and would hit the old row (_id 1, slot 0) first
        var plain = Run("upsert gs {k: 'a', etag: 'N2'} on k");
        Assert.Equal((ulong)2, plain.Data?[0]["_id"]);
    }

    [Fact]
    public void WhenNotExists_ExpiredRow_RollbackRestoresExpiredRow()
    {
        Run("upsert gs {k: 'a', etag: 'E1', ttl: 1s}");
        Thread.Sleep(1100);

        var results = _engine.Execute(
            "atomic; upsert gs {k: 'a', etag: 'N1'} on k when not exists; upsert nonexistent {x: 1}; commit",
            "testdb");
        Assert.Equal(SproutOperation.Error, Assert.Single(results).Operation);

        // Rolled back: the new row is gone and the freed key is usable again the same way
        Assert.Null(Row("a"));
        var retry = Run("upsert gs {k: 'a', etag: 'N2'} on k when not exists");
        Assert.Null(retry.Errors);
        Assert.Equal("N2", Row("a")?["etag"]);
        Assert.Equal(1, Run("get gs where k = 'a' count").Affected);
    }

    [Fact]
    public void PlainUpsertOn_ExpiredRow_IsFreedAndReplacedByNewRow()
    {
        Run("upsert gs {k: 'a', etag: 'E1', payload: 'old', ttl: 1s}");
        Thread.Sleep(1100);

        var r = Run("upsert gs {k: 'a', etag: 'N1'} on k");

        Assert.Null(r.Errors);
        Assert.Equal((ulong)2, r.Data?[0]["_id"]);
        var row = Row("a");
        Assert.NotNull(row);
        Assert.Equal("N1", row["etag"]);
        Assert.Null(row["payload"]); // not revived from the expired row
    }

    [Fact]
    public void BulkUpsertOn_ExpiredAndLiveRows_FreesOnlyTheExpired()
    {
        Run("upsert gs {k: 'a', etag: 'A1', payload: 'old', ttl: 1s}");
        Run("upsert gs {k: 'b', etag: 'B1', payload: 'keep'}");
        Thread.Sleep(1100);

        var r = Run("upsert gs [{k: 'a', etag: 'A2'}, {k: 'b', etag: 'B2'}] on k");

        Assert.Null(r.Errors);
        Assert.Equal((ulong)3, Row("a")?["_id"]);    // expired 'a' replaced by a new row
        Assert.Null(Row("a")?["payload"]);
        Assert.Equal((ulong)2, Row("b")?["_id"]);    // live 'b' updated in place
        Assert.Equal("keep", Row("b")?["payload"]);
        Assert.Equal("B2", Row("b")?["etag"]);
    }

    [Fact]
    public void WhenExists_ExpiredRow_Fails()
    {
        Run("upsert gs {k: 'a', etag: 'E1', ttl: 1s}");
        Thread.Sleep(1100);

        var exists = Run("upsert gs {k: 'a', etag: 'E2'} on k when exists");
        AssertConditionFailed(exists);
        Assert.Empty(exists.Data ?? [new Dictionary<string, object?>()]);

        var expr = Run("upsert gs {k: 'a', etag: 'E2'} on k when etag = 'E1'");
        AssertConditionFailed(expr);
    }

    // ── Syntax rules ─────────────────────────────────────────

    [Theory]
    [InlineData("upsert gs [{k: 'a'}, {k: 'b'}] on k when not exists", "bulk")]
    [InlineData("upsert gs {k: 'a', etag: 'E2'} when etag = 'E1'", "requires an 'on' clause")]
    [InlineData("upsert gs {_id: 1, etag: 'E2'} when not exists", "explicit _id")]
    [InlineData("upsert gs {k: 'a'} on k when", "after 'when'")]
    public void When_InvalidUsage_IsSyntaxError(string query, string messagePart)
    {
        var r = Run(query);

        Assert.Equal("SYNTAX_ERROR", r.Errors?[0].Code);
        Assert.Contains(messagePart, r.Errors?[0].Message ?? "");
    }

    // ── Transactions ─────────────────────────────────────────

    [Fact]
    public void ConditionFailed_InsideAtomic_RollsBackEverything_AndKeepsCurrentRow()
    {
        Run("upsert gs {k: 'a', etag: 'E1'}");
        Run("upsert gs {k: 'b', etag: 'B1'}");

        var results = _engine.Execute(
            "atomic; " +
            "upsert gs {k: 'a', etag: 'E2'} on k when etag = 'E1'; " +
            "upsert gs {k: 'b', etag: 'B2'} on k when etag = 'STALE'; " +
            "commit",
            "testdb");

        var error = Assert.Single(results);
        Assert.Equal("CONDITION_FAILED", error.Errors?[0].Code);
        Assert.StartsWith("transaction rolled back:", error.Errors?[0].Message ?? "");
        var current = Assert.Single(error.Data ?? []);
        Assert.Equal("B1", current["etag"]);

        Assert.Equal("E1", Row("a")?["etag"]);
        Assert.Equal("B1", Row("b")?["etag"]);
    }

    [Fact]
    public void ConditionFailed_InsideAtomic_ReportsCommittedRow_NotTransactionState()
    {
        Run("upsert gs {k: 'a', etag: 'E1'}");

        // First statement changes 'a' inside the transaction, second one fails on it
        var results = _engine.Execute(
            "atomic; " +
            "upsert gs {k: 'a', etag: 'TX'} on k; " +
            "upsert gs {k: 'a', etag: 'E2'} on k when etag = 'E1'; " +
            "commit",
            "testdb");

        var error = Assert.Single(results);
        Assert.Equal("CONDITION_FAILED", error.Errors?[0].Code);

        // 'TX' was rolled back — the caller must see the stored 'E1'
        var current = Assert.Single(error.Data ?? []);
        Assert.Equal("E1", current["etag"]);
    }

    [Fact]
    public void ConditionFailed_InsideAtomic_RowInsertedInTransaction_ReportsEmptyData()
    {
        var results = _engine.Execute(
            "atomic; " +
            "upsert gs {k: 'a', etag: 'TX'}; " +
            "upsert gs {k: 'a', etag: 'E2'} on k when not exists; " +
            "commit",
            "testdb");

        var error = Assert.Single(results);
        Assert.Equal("CONDITION_FAILED", error.Errors?[0].Code);
        Assert.Empty(error.Data ?? [new Dictionary<string, object?>()]);
    }

    [Fact]
    public void TwoConditionalUpserts_InsideAtomic_Commit()
    {
        Run("upsert gs {k: 'v', etag: 'V7'}");
        Run("upsert gs {k: 's1', etag: 'S1'}");

        var results = _engine.Execute(
            "atomic; " +
            "upsert gs {k: 'v', etag: 'V8'} on k when etag = 'V7'; " +
            "upsert gs {k: 's1', etag: 'S2'} on k when etag = 'S1'; " +
            "commit",
            "testdb");

        Assert.Equal(SproutOperation.Transaction, results[^1].Operation);
        Assert.Equal("V8", Row("v")?["etag"]);
        Assert.Equal("S2", Row("s1")?["etag"]);
    }

    // ── Concurrency ──────────────────────────────────────────

    [Fact]
    public void ConcurrentWriters_WithSameExpectedEtag_ExactlyOneWins()
    {
        Run("upsert gs {k: 'a', etag: 'E0'}");

        for (var round = 0; round < 20; round++)
        {
            var expected = $"E{round}";
            var next = $"E{round + 1}";
            var winners = 0;

            Parallel.For(0, 8, writer =>
            {
                var r = Run($"upsert gs {{k: 'a', etag: '{next}', payload: 'w{writer}'}} on k when etag = '{expected}'");
                if (r.Errors is null)
                    Interlocked.Increment(ref winners);
                else
                    Assert.Equal("CONDITION_FAILED", r.Errors[0].Code);
            });

            Assert.Equal(1, winners);
            Assert.Equal(next, Row("a")?["etag"]);
        }
    }

    // ── delete … expect N ────────────────────────────────────

    [Fact]
    public void DeleteExpect_NoMatch_FailsWithExpectationFailed()
    {
        Run("upsert gs {k: 'a', etag: 'E5'}");

        var r = Run("delete gs where k = 'a' and etag = 'E1' expect 1");

        Assert.Equal("EXPECTATION_FAILED", r.Errors?[0].Code);
        Assert.Equal(1, RowCount());
    }

    [Fact]
    public void DeleteExpect_TooManyMatches_DeletesNothing()
    {
        Run("upsert gs {k: 'a', etag: 'E1'}");
        Run("upsert gs {k: 'b', etag: 'E1'}");

        var r = Run("delete gs where etag = 'E1' expect 1");

        Assert.Equal("EXPECTATION_FAILED", r.Errors?[0].Code);
        Assert.Contains("found 2", r.Errors?[0].Message ?? "");
        Assert.Equal(2, RowCount());
    }

    [Fact]
    public void DeleteExpect_ExactMatch_Deletes()
    {
        Run("upsert gs {k: 'a', etag: 'E1'}");
        Run("upsert gs {k: 'b', etag: 'E1'}");

        var r = Run("delete gs where k = 'a' and etag = 'E1' expect 1");

        Assert.Null(r.Errors);
        Assert.Equal(1, r.Affected);
        Assert.Null(Row("a"));
        Assert.NotNull(Row("b"));
    }

    [Fact]
    public void DeleteExpect_Zero_PassesWhenNothingMatches()
    {
        var r = Run("delete gs where k = 'missing' expect 0");

        Assert.Null(r.Errors);
        Assert.Equal(0, r.Affected);
    }

    [Fact]
    public void DeleteExpect_IgnoresExpiredRows()
    {
        Run("upsert gs {k: 'a', etag: 'E1', ttl: 1s}");
        Run("upsert gs {k: 'b', etag: 'E1'}");
        Thread.Sleep(1100);

        // Only 'b' is alive — the expired 'a' is neither counted nor deleted
        var r = Run("delete gs where etag = 'E1' expect 1");

        Assert.Null(r.Errors);
        Assert.Equal(1, r.Affected);
        Assert.Null(Row("b"));

        var expiredOnly = Run("delete gs where k = 'a' expect 1");
        Assert.Equal("EXPECTATION_FAILED", expiredOnly.Errors?[0].Code);
    }

    [Fact]
    public void DeleteExpect_InsideAtomic_RollsBack()
    {
        Run("upsert gs {k: 'a', etag: 'E1'}");

        var results = _engine.Execute(
            "atomic; upsert gs {k: 'a', etag: 'E2'} on k; delete gs where k = 'zzz' expect 1; commit",
            "testdb");

        Assert.Equal("EXPECTATION_FAILED", Assert.Single(results).Errors?[0].Code);
        Assert.Equal("E1", Row("a")?["etag"]);
    }

    [Theory]
    [InlineData("delete gs where k = 'a' expect")]
    [InlineData("delete gs where k = 'a' expect -1")]
    [InlineData("delete gs where k = 'a' expect 'x'")]
    public void DeleteExpect_InvalidCount_IsSyntaxError(string query)
    {
        Assert.Equal("SYNTAX_ERROR", Run(query).Errors?[0].Code);
    }

    // ── Typed API ────────────────────────────────────────────

    [Fact]
    public void TypedApi_UpsertWhen()
    {
        var db = _engine.GetOrCreateDatabase("testdb");
        var table = db.Table<GrainState>("gs");
        table.Upsert(new GrainState { K = "a", Etag = "E1" });

        var expected = "E1";
        var ok = table.Upsert(new GrainState { K = "a", Etag = "E2" }, on: g => g.K, when: g => g.Etag == expected);
        Assert.Null(ok.Errors);

        var stale = table.Upsert(new GrainState { K = "a", Etag = "E3" }, on: g => g.K, when: g => g.Etag == expected);
        Assert.Equal("CONDITION_FAILED", stale.Errors?[0].Code);
        Assert.Equal("E2", Assert.Single(stale.Data ?? [])["etag"]);
    }

    [Fact]
    public void TypedApi_UpsertIfNotExists()
    {
        var table = _engine.GetOrCreateDatabase("testdb").Table<GrainState>("gs");

        var first = table.Upsert(new GrainState { K = "a", Etag = "E1" }, on: g => g.K, ifNotExists: true);
        Assert.Null(first.Errors);

        var second = table.Upsert(new GrainState { K = "a", Etag = "X" }, on: g => g.K, ifNotExists: true);
        Assert.Equal("CONDITION_FAILED", second.Errors?[0].Code);
    }

    [Fact]
    public void TypedApi_DeleteExpect()
    {
        var table = _engine.GetOrCreateDatabase("testdb").Table<GrainState>("gs");
        table.Upsert(new GrainState { K = "a", Etag = "E1" });

        var stale = table.Delete(g => g.K == "a" && g.Etag == "E0", expect: 1);
        Assert.Equal("EXPECTATION_FAILED", stale.Errors?[0].Code);

        var ok = table.Delete(g => g.K == "a" && g.Etag == "E1", expect: 1);
        Assert.Null(ok.Errors);
        Assert.Equal(1, ok.Affected);
    }
}

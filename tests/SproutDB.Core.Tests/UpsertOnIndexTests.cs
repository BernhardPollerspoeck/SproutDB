namespace SproutDB.Core.Tests;

/// <summary>
/// <c>upsert … on COL</c> resolves rows via the B-Tree when COL is indexed.
/// The result must be identical to the full scan used for unindexed columns.
/// </summary>
public class UpsertOnIndexTests : IDisposable
{
    private readonly string _tempDir;
    private readonly SproutEngine _engine;

    public UpsertOnIndexTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), $"sproutdb-test-{Guid.NewGuid()}");
        _engine = new SproutEngine(_tempDir);
        _engine.ExecuteOne("create database", "testdb");
        // Same data in an indexed (b-tree path) and an unindexed (scan path) table
        _engine.ExecuteOne("create table idx (k string 16, v sint)", "testdb");
        _engine.ExecuteOne("create index idx.k", "testdb");
        _engine.ExecuteOne("create table scan (k string 16, v sint)", "testdb");
    }

    public void Dispose()
    {
        _engine.Dispose();
        if (Directory.Exists(_tempDir))
            Directory.Delete(_tempDir, true);
    }

    private void Both(string queryTemplate)
    {
        _engine.ExecuteOne(queryTemplate.Replace("$t", "idx"), "testdb");
        _engine.ExecuteOne(queryTemplate.Replace("$t", "scan"), "testdb");
    }

    private List<(ulong Id, string? K, int? V)> Rows(string table)
    {
        var r = _engine.ExecuteOne($"get {table} select _id, k, v order by _id", "testdb");
        return (r.Data ?? []).Select(row => ((ulong)(row["_id"] ?? 0UL), row["k"] as string, row["v"] as int?)).ToList();
    }

    private void AssertSameResult() => Assert.Equal(Rows("scan"), Rows("idx"));

    [Fact]
    public void Match_UpdatesSameRow()
    {
        Both("upsert $t [{k: 'a', v: 1}, {k: 'b', v: 2}]");
        Both("upsert $t {k: 'b', v: 20} on k");

        AssertSameResult();
        Assert.Equal(20, Rows("idx")[1].V);
    }

    [Fact]
    public void NoMatch_Inserts()
    {
        Both("upsert $t {k: 'a', v: 1}");
        Both("upsert $t {k: 'z', v: 9} on k");

        AssertSameResult();
        Assert.Equal(2, Rows("idx").Count);
    }

    [Fact]
    public void DuplicateValues_LowestSlotWins()
    {
        // Non-unique index: several rows carry the same key
        Both("upsert $t [{k: 'd', v: 1}, {k: 'd', v: 2}, {k: 'd', v: 3}]");
        Both("upsert $t {k: 'd', v: 100} on k");

        AssertSameResult();
        Assert.Equal(100, Rows("idx")[0].V);
    }

    [Fact]
    public void NullKey_UsesScanFallback()
    {
        Both("upsert $t [{v: 1}, {k: 'a', v: 2}]");
        Both("upsert $t {k: null, v: 10} on k");

        AssertSameResult();
        Assert.Equal(10, Rows("idx")[0].V);
    }

    [Fact]
    public void BulkWithDuplicateKeysInBatch_SameAsScan()
    {
        Both("upsert $t {k: 'a', v: 1}");
        Both("upsert $t [{k: 'a', v: 5}, {k: 'a', v: 6}, {k: 'n', v: 7}] on k");

        AssertSameResult();
    }

    [Fact]
    public void AfterDeleteAndReinsert_IndexStaysConsistent()
    {
        Both("upsert $t [{k: 'a', v: 1}, {k: 'b', v: 2}]");
        Both("delete $t where k = 'a'");
        Both("upsert $t {k: 'a', v: 3} on k");
        Both("upsert $t {k: 'a', v: 4} on k");

        AssertSameResult();
        Assert.Single(Rows("idx"), r => r.K == "a");
    }

    [Fact]
    public void AfterRolledBackTransaction_IndexStaysConsistent()
    {
        Both("upsert $t {k: 'a', v: 1}");
        foreach (var t in new[] { "idx", "scan" })
            _engine.Execute($"atomic; upsert {t} {{k: 'a', v: 2}} on k; upsert {t} {{k: 'x', v: 1}} on k; upsert nonexistent {{x: 1}}; commit", "testdb");
        Both("upsert $t {k: 'x', v: 5} on k");
        Both("upsert $t {k: 'a', v: 6} on k");

        AssertSameResult();
        Assert.Equal([(1UL, "a", (int?)6), (2UL, "x", (int?)5)], Rows("idx"));
    }

    [Fact]
    public void LargeTable_UpsertOnIndexedColumn_IsCorrect()
    {
        _engine.ExecuteOne("upsert idx [" + string.Join(", ", Enumerable.Range(0, 100).Select(i => $"{{k: 'k{i}', v: {i}}}")) + "]", "testdb");

        _engine.ExecuteOne("upsert idx {k: 'k77', v: -1} on k", "testdb");

        var r = _engine.ExecuteOne("get idx select _id, v where k = 'k77'", "testdb");
        var row = Assert.Single(r.Data ?? []);
        Assert.Equal((ulong)78, row["_id"]);
        Assert.Equal(-1, row["v"]);
    }
}

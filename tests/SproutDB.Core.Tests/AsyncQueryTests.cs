namespace SproutDB.Core.Tests;

/// <summary>
/// <c>QueryAsync</c> / <c>ExecuteAsync</c>: non-blocking writes, synchronous reads,
/// and cancellation that only ever skips writes that have not started.
/// </summary>
public class AsyncQueryTests : IDisposable
{
    private readonly string _tempDir;
    private readonly SproutEngine _engine;
    private readonly ISproutDatabase _db;

    public AsyncQueryTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), $"sproutdb-test-{Guid.NewGuid()}");
        _engine = new SproutEngine(_tempDir);
        _db = _engine.GetOrCreateDatabase("testdb");
        _db.Query("create table gs (k string 64 unique, etag string 16)");
    }

    public void Dispose()
    {
        _engine.Dispose();
        if (Directory.Exists(_tempDir))
            Directory.Delete(_tempDir, true);
    }

    private int RowCount() => _db.Query("get gs")[0].Data?.Count ?? 0;

    /// <summary>Occupies the single writer until the returned gate is released.</summary>
    private (SemaphoreSlim Gate, Task Blocker) BlockWriter()
    {
        var gate = new SemaphoreSlim(0);
        var started = new ManualResetEventSlim();
        var blocker = _engine.EnqueueWrite(() =>
        {
            started.Set();
            gate.Wait();
            return new SproutResponse { Operation = SproutOperation.Get };
        }, CancellationToken.None);
        started.Wait();
        return (gate, blocker);
    }

    [Fact]
    public async Task QueryAsync_Write_CompletesWithResult()
    {
        var results = await _db.QueryAsync("upsert gs {k: 'a', etag: 'E1'}");

        Assert.Null(results[0].Errors);
        Assert.Equal(1, RowCount());
    }

    [Fact]
    public void QueryAsync_Read_CompletesSynchronously()
    {
        _db.Query("upsert gs {k: 'a'}");

        var pending = _db.QueryAsync("get gs");

        Assert.True(pending.IsCompletedSuccessfully);
        Assert.Single(pending.Result[0].Data ?? []);
    }

    [Fact]
    public async Task QueryAsync_DoesNotBlockWhileWriterIsBusy()
    {
        var (gate, blocker) = BlockWriter();

        var pending = _db.QueryAsync("upsert gs {k: 'a'}").AsTask();
        Assert.False(pending.IsCompleted); // queued behind the blocker, caller not blocked

        gate.Release();
        await blocker;
        var results = await pending;
        Assert.Null(results[0].Errors);
    }

    [Fact]
    public async Task QueryAsync_CancelledBeforeCall_ThrowsAndWritesNothing()
    {
        using var cts = new CancellationTokenSource();
        cts.Cancel();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(
            async () => await _db.QueryAsync("upsert gs {k: 'a'}", cts.Token));
        Assert.Equal(0, RowCount());
    }

    [Fact]
    public async Task QueryAsync_CancelledWhileQueued_IsNeverExecuted()
    {
        var (gate, blocker) = BlockWriter();
        using var cts = new CancellationTokenSource();

        var pending = _db.QueryAsync("upsert gs {k: 'a'}", cts.Token).AsTask();
        cts.Cancel();
        gate.Release();
        await blocker;

        await Assert.ThrowsAnyAsync<OperationCanceledException>(async () => await pending);
        Assert.Equal(0, RowCount());
    }

    [Fact]
    public async Task QueryAsync_Transaction()
    {
        var results = await _db.QueryAsync("atomic; upsert gs {k: 'a'}; upsert gs {k: 'b'}; commit");

        Assert.Equal(SproutOperation.Transaction, results[^1].Operation);
        Assert.Equal(2, RowCount());
    }

    [Fact]
    public async Task QueryAsync_WithParameters()
    {
        await _db.QueryAsync("upsert gs {k: @k, etag: @e}", new { k = "a'b", e = "E1" });

        var results = await _db.QueryAsync("get gs select etag where k = @k", new { k = "a'b" });

        Assert.Equal("E1", Assert.Single(results[0].Data ?? [])["etag"]);
    }

    [Fact]
    public async Task QueryAsync_ParameterError_IsReturnedNotThrown()
    {
        var results = await _db.QueryAsync("get gs where k = @missing", new { other = 1 });

        Assert.Equal("PARAMETER_ERROR", results[0].Errors?[0].Code);
    }

    [Fact]
    public async Task QueryAsync_ConcurrentConditionalWriters_ExactlyOneWins()
    {
        await _db.QueryAsync("upsert gs {k: 'a', etag: 'E1'}");

        var results = await Task.WhenAll(Enumerable.Range(0, 20).Select(i =>
            _db.QueryAsync("upsert gs {k: 'a', etag: @next} on k when etag = 'E1'", new { next = $"W{i}" }).AsTask()));

        Assert.Equal(1, results.Count(r => r[0].Errors is null));
        Assert.Equal(19, results.Count(r => r[0].Errors?[0].Code == "CONDITION_FAILED"));
    }

    [Fact]
    public async Task QueryAsync_MixedBatch_KeepsOrder()
    {
        var results = await _db.QueryAsync("upsert gs {k: 'a'}; get gs; upsert gs {k: 'b'}; get gs");

        Assert.Equal(4, results.Count);
        Assert.Single(results[1].Data ?? []);
        Assert.Equal(2, results[3].Data?.Count);
    }
}

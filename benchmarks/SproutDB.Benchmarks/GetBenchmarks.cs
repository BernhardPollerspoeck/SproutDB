using BenchmarkDotNet.Attributes;
using SproutDB.Core;

namespace SproutDB.Benchmarks;

[MemoryDiagnoser]
[ShortRunJob]
public class GetBenchmarks
{
    private string _tempDir = null!;
    private SproutEngine _engine = null!;

    [Params(100, 10_000, 1_000_000)]
    public int RowCount;

    [GlobalSetup]
    public void Setup()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), $"sproutdb-bench-get-{Guid.NewGuid()}");
        _engine = new SproutEngine(new SproutEngineSettings
        {
            DataDirectory = _tempDir,
            FlushInterval = Timeout.InfiniteTimeSpan,
        });

        _engine.Execute("create database", "bench");
        _engine.Execute(
            "create table users (name string 100, email string 320, age ubyte, active bool default true, score sint)",
            "bench");

        for (var i = 0; i < RowCount; i++)
        {
            _engine.Execute(
                $"upsert users {{name: 'User{i}', email: 'user{i}@test.com', age: {i % 100}, score: {i}}}",
                "bench");
        }
    }

    [GlobalCleanup]
    public void Cleanup()
    {
        _engine.Dispose();
        if (Directory.Exists(_tempDir))
            Directory.Delete(_tempDir, true);
    }

    [Benchmark(Description = "Get: all columns")]
    public SproutResponse GetAll()
    {
        return _engine.Execute("get users", "bench")[0];
    }

    [Benchmark(Description = "Get: select 2 columns")]
    public SproutResponse GetSelect2()
    {
        return _engine.Execute("get users select name, score", "bench")[0];
    }

    [Benchmark(Description = "Get: select id only")]
    public SproutResponse GetSelectId()
    {
        return _engine.Execute("get users select id", "bench")[0];
    }
}

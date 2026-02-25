using BenchmarkDotNet.Attributes;
using SproutDB.Core;

namespace SproutDB.Benchmarks;

[MemoryDiagnoser]
[ShortRunJob]
public class InsertThroughputBenchmarks
{
    private string _tempDir = null!;
    private SproutEngine _engine = null!;

    [GlobalSetup]
    public void Setup()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), $"sproutdb-bench-insert-{Guid.NewGuid()}");
        _engine = new SproutEngine(new SproutEngineSettings
        {
            DataDirectory = _tempDir,
            FlushInterval = Timeout.InfiniteTimeSpan,
        });
        _engine.Execute("create database", "bench");
        _engine.Execute(
            "create table users (name string 100, email string 320, age ubyte, active bool default true, score sint)",
            "bench");
    }

    [GlobalCleanup]
    public void Cleanup()
    {
        _engine.Dispose();
        if (Directory.Exists(_tempDir))
            Directory.Delete(_tempDir, true);
    }

    [Benchmark(Description = "Insert: single row (3 fields)", OperationsPerInvoke = 100)]
    public void Insert_100Rows_3Fields()
    {
        for (var i = 0; i < 100; i++)
            _engine.Execute("upsert users {name: 'Bench', age: 25, score: 42}", "bench");
    }

    [Benchmark(Description = "Insert: single row (5 fields)", OperationsPerInvoke = 100)]
    public void Insert_100Rows_5Fields()
    {
        for (var i = 0; i < 100; i++)
            _engine.Execute("upsert users {name: 'Bench', email: 'bench@test.com', age: 25, active: false, score: 42}", "bench");
    }

    [Benchmark(Description = "Insert: single row (empty)", OperationsPerInvoke = 100)]
    public void Insert_100Rows_Empty()
    {
        for (var i = 0; i < 100; i++)
            _engine.Execute("upsert users {}", "bench");
    }
}

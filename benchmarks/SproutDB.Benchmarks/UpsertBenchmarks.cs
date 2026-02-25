using BenchmarkDotNet.Attributes;
using SproutDB.Core;

namespace SproutDB.Benchmarks;

[MemoryDiagnoser]
[ShortRunJob]
public class UpsertBenchmarks
{
    private string _tempDir = null!;
    private SproutEngine _engine = null!;
    private ulong _nextUpdateId;

    [GlobalSetup]
    public void Setup()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), $"sproutdb-bench-{Guid.NewGuid()}");
        _engine = new SproutEngine(new SproutEngineSettings
        {
            DataDirectory = _tempDir,
            FlushInterval = Timeout.InfiniteTimeSpan,
        });
        _engine.Execute("create database", "bench");
        _engine.Execute(
            "create table users (name string 100, email string 320, age ubyte, active bool default true, score sint)",
            "bench");

        // Pre-populate rows for update benchmarks
        for (var i = 0; i < 1000; i++)
        {
            _engine.Execute($"upsert users {{name: 'User{i}', email: 'user{i}@test.com', age: 25, score: {i}}}", "bench");
        }
        _nextUpdateId = 1;
    }

    [GlobalCleanup]
    public void Cleanup()
    {
        _engine.Dispose();
        if (Directory.Exists(_tempDir))
            Directory.Delete(_tempDir, true);
    }

    [Benchmark(Description = "Insert: 5 fields")]
    public SproutResponse Insert_5Fields()
    {
        return _engine.Execute("upsert users {name: 'Bench', email: 'bench@test.com', age: 30, active: false, score: 999}", "bench");
    }

    [Benchmark(Description = "Insert: empty record")]
    public SproutResponse Insert_Empty()
    {
        return _engine.Execute("upsert users {}", "bench");
    }

    [Benchmark(Description = "Update: 1 field")]
    public SproutResponse Update_1Field()
    {
        var id = _nextUpdateId++;
        if (_nextUpdateId > 1000) _nextUpdateId = 1;
        return _engine.Execute($"upsert users {{id: {id}, name: 'Updated'}}", "bench");
    }

    [Benchmark(Description = "Update: 3 fields")]
    public SproutResponse Update_3Fields()
    {
        var id = _nextUpdateId++;
        if (_nextUpdateId > 1000) _nextUpdateId = 1;
        return _engine.Execute($"upsert users {{id: {id}, name: 'Updated', age: 99, score: -1}}", "bench");
    }
}

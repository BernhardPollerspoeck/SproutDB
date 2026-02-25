using BenchmarkDotNet.Attributes;
using SproutDB.Core;

namespace SproutDB.Benchmarks;

[MemoryDiagnoser]
[ShortRunJob]
public class EndToEndBenchmarks
{
    private string _tempDir = null!;
    private SproutEngine _engine = null!;

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
    }

    [GlobalCleanup]
    public void Cleanup()
    {
        _engine.Dispose();
        if (Directory.Exists(_tempDir))
            Directory.Delete(_tempDir, true);
    }

    [Benchmark(Description = "E2E: parse + execute create database (error: exists)")]
    public SproutResponse CreateDatabase_AlreadyExists()
    {
        return _engine.Execute("create database", "bench");
    }

    [Benchmark(Description = "E2E: parse + execute (unknown command)")]
    public SproutResponse UnknownCommand()
    {
        return _engine.Execute("select * from users", "bench");
    }

    [Benchmark(Description = "E2E: parse + execute (invalid db name)")]
    public SproutResponse InvalidDbName()
    {
        return _engine.Execute("create database", "123bad");
    }
}

using BenchmarkDotNet.Attributes;
using SproutDB.Core;
using SproutDB.Core.Execution;
using SproutDB.Core.Parsing;
using SproutDB.Core.Storage;

namespace SproutDB.Benchmarks;

/// <summary>
/// Breaks down the insert pipeline into individual stages to identify bottlenecks.
/// Stages: Parse → WAL append → MMF write → Response build.
/// </summary>
[MemoryDiagnoser]
[ShortRunJob]
public class InsertPipelineBenchmarks
{
    private const string Query3Fields = "upsert users {name: 'Bench', age: 25, score: 42}";
    private const string Query5Fields = "upsert users {name: 'Bench', email: 'bench@test.com', age: 25, active: false, score: 42}";

    private string _tempDir = null!;
    private SproutEngine _engine = null!;
    private WalFile _wal = null!;
    private TableHandle _table = null!;

    // Pre-parsed queries for stages that skip parsing
    private UpsertQuery _parsed3Fields = null!;
    private UpsertQuery _parsed5Fields = null!;

    [GlobalSetup]
    public void Setup()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), $"sproutdb-bench-pipeline-{Guid.NewGuid()}");
        _engine = new SproutEngine(new SproutEngineSettings
        {
            DataDirectory = _tempDir,
            FlushInterval = Timeout.InfiniteTimeSpan,
        });
        _engine.Execute("create database", "bench");
        _engine.Execute(
            "create table users (name string 100, email string 320, age ubyte, active bool default true, score sint)",
            "bench");

        // Open handles directly for isolated stage benchmarks
        var dbPath = Path.Combine(_tempDir, "bench");
        var walPath = Path.Combine(dbPath, "_wal");
        _wal = new WalFile(walPath + ".bench");
        _table = TableHandle.Open(Path.Combine(dbPath, "users"));

        // Pre-parse queries
        var result3 = QueryParser.Parse(Query3Fields);
        if (result3.Query is UpsertQuery q3)
            _parsed3Fields = q3;

        var result5 = QueryParser.Parse(Query5Fields);
        if (result5.Query is UpsertQuery q5)
            _parsed5Fields = q5;
    }

    [GlobalCleanup]
    public void Cleanup()
    {
        _wal.Dispose();
        _table.Dispose();
        _engine.Dispose();
        if (Directory.Exists(_tempDir))
            Directory.Delete(_tempDir, true);
    }

    // ── Stage 1: Parse only ─────────────────────────────────

    [Benchmark(Description = "1. Parse only (3 fields)")]
    public bool Parse_3Fields()
    {
        return QueryParser.Parse(Query3Fields).Success;
    }

    [Benchmark(Description = "1. Parse only (5 fields)")]
    public bool Parse_5Fields()
    {
        return QueryParser.Parse(Query5Fields).Success;
    }

    // ── Stage 2: WAL append only (buffer write, no fsync) ───

    [Benchmark(Description = "2. WAL append (buffer only)")]
    public long WalAppend()
    {
        return _wal.Append(Query3Fields);
    }

    // ── Stage 3: MMF write only (skip parse + WAL) ──────────

    [Benchmark(Description = "3. MMF write only (3 fields)")]
    public SproutResponse MmfWrite_3Fields()
    {
        return UpsertExecutor.Execute(Query3Fields, _table, _parsed3Fields, 100);
    }

    [Benchmark(Description = "3. MMF write only (5 fields)")]
    public SproutResponse MmfWrite_5Fields()
    {
        return UpsertExecutor.Execute(Query5Fields, _table, _parsed5Fields, 100);
    }

    // ── Full pipeline (for comparison) ──────────────────────

    [Benchmark(Description = "4. Full pipeline (3 fields)", Baseline = true)]
    public SproutResponse FullPipeline_3Fields()
    {
        return _engine.Execute(Query3Fields, "bench");
    }

    [Benchmark(Description = "4. Full pipeline (5 fields)")]
    public SproutResponse FullPipeline_5Fields()
    {
        return _engine.Execute(Query5Fields, "bench");
    }
}

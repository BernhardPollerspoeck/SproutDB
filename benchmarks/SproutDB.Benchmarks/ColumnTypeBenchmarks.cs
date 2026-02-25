using BenchmarkDotNet.Attributes;

namespace SproutDB.Benchmarks;

[MemoryDiagnoser]
[ShortRunJob]
public class ColumnTypeBenchmarks
{
    [Benchmark(Description = "TryParse: string")]
    public bool TryParse_String() => SproutDB.Core.ColumnTypes.TryParse("string", out _);

    [Benchmark(Description = "TryParse: ubyte")]
    public bool TryParse_UByte() => SproutDB.Core.ColumnTypes.TryParse("ubyte", out _);

    [Benchmark(Description = "TryParse: datetime")]
    public bool TryParse_DateTime() => SproutDB.Core.ColumnTypes.TryParse("datetime", out _);

    [Benchmark(Description = "TryParse: unknown")]
    public bool TryParse_Unknown() => SproutDB.Core.ColumnTypes.TryParse("varchar", out _);
}

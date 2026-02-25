using BenchmarkDotNet.Attributes;
using SproutDB.Core.Parsing;

namespace SproutDB.Benchmarks;

[MemoryDiagnoser]
[ShortRunJob]
public class TokenizerBenchmarks
{
    private const string SHORT_QUERY = "create table users";
    private const string MEDIUM_QUERY = "upsert users {name: 'John', email: 'john@test.com', age: 25, active: true}";
    private const string LONG_QUERY = "create table users (name string 100, email string 320 strict, age ubyte, active bool default true, score sint, bio string 5000, created date)";

    [Benchmark(Description = "Short: create table users")]
    public int Tokenize_Short() => Tokenizer.Tokenize(SHORT_QUERY).Count;

    [Benchmark(Description = "Medium: upsert with 4 fields")]
    public int Tokenize_Medium() => Tokenizer.Tokenize(MEDIUM_QUERY).Count;

    [Benchmark(Description = "Long: create table 7 columns")]
    public int Tokenize_Long() => Tokenizer.Tokenize(LONG_QUERY).Count;
}

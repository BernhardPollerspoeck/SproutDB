using BenchmarkDotNet.Attributes;
using SproutDB.Core.Parsing;

namespace SproutDB.Benchmarks;

[MemoryDiagnoser]
[ShortRunJob]
public class ParserBenchmarks
{
    [Benchmark(Description = "Parse: create database")]
    public bool Parse_CreateDatabase() => QueryParser.Parse("create database").Success;

    [Benchmark(Description = "Parse: create table (empty)")]
    public bool Parse_CreateTable_Empty() => QueryParser.Parse("create table users").Success;

    [Benchmark(Description = "Parse: create table (5 cols)")]
    public bool Parse_CreateTable_5Cols() =>
        QueryParser.Parse("create table users (name string 100, email string 320 strict, age ubyte, active bool default true, score sint)").Success;

    [Benchmark(Description = "Parse: upsert (4 fields)")]
    public bool Parse_Upsert() =>
        QueryParser.Parse("upsert users {name: 'John', email: 'john@test.com', age: 25, active: true}").Success;

    [Benchmark(Description = "Parse: upsert (empty)")]
    public bool Parse_Upsert_Empty() => QueryParser.Parse("upsert users {}").Success;

    [Benchmark(Description = "Parse: add column")]
    public bool Parse_AddColumn() => QueryParser.Parse("add column users.premium bool default false").Success;

    [Benchmark(Description = "Parse: error (unknown cmd)")]
    public bool Parse_Error() => QueryParser.Parse("select * from users").Success;
}

namespace SproutDB.Core.Tests;

public class TypeWideningTests : IDisposable
{
    private readonly string _tempDir;
    private readonly SproutEngine _engine;

    public TypeWideningTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), $"sproutdb-test-{Guid.NewGuid()}");
        _engine = new SproutEngine(_tempDir);
        _engine.Execute("create database", "testdb");
    }

    public void Dispose()
    {
        _engine.Dispose();
        if (Directory.Exists(_tempDir))
            Directory.Delete(_tempDir, true);
    }

    // ── Unsigned chain: ubyte → ushort → uint → ulong ────

    [Fact]
    public void Widen_UByte_To_UShort_PreservesData()
    {
        _engine.Execute("create table t1 (val ubyte)", "testdb");
        _engine.Execute("upsert t1 {val: 42}", "testdb");
        _engine.Execute("upsert t1 {val: 200}", "testdb");

        // Widen to ushort
        _engine.Execute("add column t1.val ushort", "testdb");

        var result = _engine.Execute("get t1", "testdb");
        Assert.NotNull(result.Data);
        Assert.Equal(2, result.Data.Count);

        var values = result.Data.Select(r => Convert.ToInt64(r["val"])).OrderBy(v => v).ToList();
        Assert.Equal(42, values[0]);
        Assert.Equal(200, values[1]);
    }

    [Fact]
    public void Widen_UShort_To_UInt_PreservesData()
    {
        _engine.Execute("create table t2 (val ushort)", "testdb");
        _engine.Execute("upsert t2 {val: 1000}", "testdb");
        _engine.Execute("upsert t2 {val: 50000}", "testdb");

        _engine.Execute("add column t2.val uint", "testdb");

        var result = _engine.Execute("get t2", "testdb");
        Assert.NotNull(result.Data);
        var values = result.Data.Select(r => Convert.ToInt64(r["val"])).OrderBy(v => v).ToList();
        Assert.Equal(1000, values[0]);
        Assert.Equal(50000, values[1]);
    }

    [Fact]
    public void Widen_UInt_To_ULong_PreservesData()
    {
        _engine.Execute("create table t3 (val uint)", "testdb");
        _engine.Execute("upsert t3 {val: 123456}", "testdb");

        _engine.Execute("add column t3.val ulong", "testdb");

        var result = _engine.Execute("get t3", "testdb");
        Assert.NotNull(result.Data);
        Assert.Single(result.Data);
        Assert.Equal(123456UL, Convert.ToUInt64(result.Data[0]["val"]));
    }

    // ── Signed chain: sbyte → sshort → sint → slong ──────

    [Fact]
    public void Widen_SByte_To_SShort_PreservesNegativeValues()
    {
        _engine.Execute("create table t4 (val sbyte)", "testdb");
        _engine.Execute("upsert t4 {val: -50}", "testdb");
        _engine.Execute("upsert t4 {val: 100}", "testdb");

        _engine.Execute("add column t4.val sshort", "testdb");

        var result = _engine.Execute("get t4", "testdb");
        Assert.NotNull(result.Data);
        var values = result.Data.Select(r => Convert.ToInt64(r["val"])).OrderBy(v => v).ToList();
        Assert.Equal(-50, values[0]);
        Assert.Equal(100, values[1]);
    }

    // ── Float → Double ───────────────────────────────────

    [Fact]
    public void Widen_Float_To_Double_PreservesData()
    {
        _engine.Execute("create table t5 (val float)", "testdb");
        _engine.Execute("upsert t5 {val: 3.14}", "testdb");
        _engine.Execute("upsert t5 {val: -1.5}", "testdb");

        _engine.Execute("add column t5.val double", "testdb");

        var result = _engine.Execute("get t5", "testdb");
        Assert.NotNull(result.Data);
        var values = result.Data.Select(r => Convert.ToDouble(r["val"])).OrderBy(v => v).ToList();
        Assert.Equal(-1.5, values[0], 2);
        Assert.Equal(3.14, values[1], 2);
    }

    // ── Null values survive widening ─────────────────────

    [Fact]
    public void Widen_WithNullValues_PreservesNulls()
    {
        _engine.Execute("create table t6 (name string 50, val ubyte)", "testdb");
        _engine.Execute("upsert t6 {name: 'with_val', val: 99}", "testdb");
        _engine.Execute("upsert t6 {name: 'no_val'}", "testdb");

        _engine.Execute("add column t6.val ushort", "testdb");

        var result = _engine.Execute("get t6", "testdb");
        Assert.NotNull(result.Data);
        Assert.Equal(2, result.Data.Count);

        var withVal = result.Data.FirstOrDefault(r => r["name"]?.ToString() == "with_val");
        var noVal = result.Data.FirstOrDefault(r => r["name"]?.ToString() == "no_val");
        Assert.NotNull(withVal);
        Assert.NotNull(noVal);
        Assert.Equal(99, Convert.ToInt64(withVal["val"]));
        Assert.Null(noVal["val"]);
    }

    // ── WHERE still works after widening ─────────────────

    [Fact]
    public void Widen_Where_StillWorksAfterRebuild()
    {
        _engine.Execute("create table t7 (score ubyte)", "testdb");
        _engine.Execute("upsert t7 {score: 10}", "testdb");
        _engine.Execute("upsert t7 {score: 20}", "testdb");
        _engine.Execute("upsert t7 {score: 30}", "testdb");

        _engine.Execute("add column t7.score ushort", "testdb");

        var result = _engine.Execute("get t7 where score > 15", "testdb");
        Assert.NotNull(result.Data);
        Assert.Equal(2, result.Data.Count);
    }

    // ── Invalid widening is rejected ─────────────────────

    [Fact]
    public void Widen_UByteToSByte_IsRejected()
    {
        _engine.Execute("create table t8 (val ubyte)", "testdb");
        var result = _engine.Execute("add column t8.val sbyte", "testdb");
        Assert.Equal(SproutOperation.Error, result.Operation);
    }

    [Fact]
    public void Widen_DoubleToFloat_IsRejected()
    {
        _engine.Execute("create table t9 (val double)", "testdb");
        var result = _engine.Execute("add column t9.val float", "testdb");
        Assert.Equal(SproutOperation.Error, result.Operation);
    }

    // ── Schema correctly reflects new type ───────────────

    [Fact]
    public void Widen_SchemaReflectsNewType()
    {
        _engine.Execute("create table t10 (val ubyte)", "testdb");
        _engine.Execute("add column t10.val ushort", "testdb");

        var result = _engine.Execute("describe t10", "testdb");
        var cols = result.Schema?.Columns;
        Assert.NotNull(cols);
        var valCol = cols.FirstOrDefault(c => c.Name == "val");
        Assert.NotNull(valCol);
        Assert.Equal("ushort", valCol.Type);
    }
}

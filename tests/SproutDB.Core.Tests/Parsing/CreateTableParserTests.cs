using SproutDB.Core.Parsing;

namespace SproutDB.Core.Tests.Parsing;

public class CreateTableParserTests
{
    // ── Success cases ────────────────────────────────────────

    [Fact]
    public void EmptyTable()
    {
        var result = QueryParser.Parse("create table users");

        Assert.True(result.Success);
        var q = Assert.IsType<CreateTableQuery>(result.Query);
        Assert.Equal("users", q.Table);
        Assert.Empty(q.Columns);
    }

    [Fact]
    public void TableName_IsLowercased()
    {
        var q = Assert.IsType<CreateTableQuery>(QueryParser.Parse("create table Users").Query);
        Assert.Equal("users", q.Table);
    }

    [Fact]
    public void SingleColumn()
    {
        var result = QueryParser.Parse("create table users (name string)");
        var q = Assert.IsType<CreateTableQuery>(result.Query);

        Assert.Single(q.Columns);
        Assert.Equal("name", q.Columns[0].Name);
        Assert.Equal(ColumnType.String, q.Columns[0].Type);
        Assert.Equal(255, q.Columns[0].Size);
        Assert.True(q.Columns[0].IsNullable);
        Assert.False(q.Columns[0].Strict);
        Assert.Null(q.Columns[0].Default);
    }

    [Fact]
    public void MultipleColumns()
    {
        var result = QueryParser.Parse(
            "create table users (name string, email string 320 strict, age ubyte, active bool default true, bio string 5000, created date)");
        var q = Assert.IsType<CreateTableQuery>(result.Query);

        Assert.Equal(6, q.Columns.Count);

        Assert.Equal("name", q.Columns[0].Name);
        Assert.Equal(ColumnType.String, q.Columns[0].Type);
        Assert.Equal(255, q.Columns[0].Size);

        Assert.Equal("email", q.Columns[1].Name);
        Assert.Equal(ColumnType.String, q.Columns[1].Type);
        Assert.Equal(320, q.Columns[1].Size);
        Assert.True(q.Columns[1].Strict);

        Assert.Equal("age", q.Columns[2].Name);
        Assert.Equal(ColumnType.UByte, q.Columns[2].Type);
        Assert.Equal(1, q.Columns[2].Size);

        Assert.Equal("active", q.Columns[3].Name);
        Assert.Equal(ColumnType.Bool, q.Columns[3].Type);
        Assert.Equal("true", q.Columns[3].Default);
        Assert.False(q.Columns[3].IsNullable);

        Assert.Equal("bio", q.Columns[4].Name);
        Assert.Equal(5000, q.Columns[4].Size);

        Assert.Equal("created", q.Columns[5].Name);
        Assert.Equal(ColumnType.Date, q.Columns[5].Type);
    }

    [Fact]
    public void DefaultBeforeStrict()
    {
        var result = QueryParser.Parse("create table t (x sint default 0 strict)");
        var q = Assert.IsType<CreateTableQuery>(result.Query);

        Assert.Equal("0", q.Columns[0].Default);
        Assert.True(q.Columns[0].Strict);
    }

    [Fact]
    public void StrictBeforeDefault()
    {
        var result = QueryParser.Parse("create table t (x sint strict default 42)");
        var q = Assert.IsType<CreateTableQuery>(result.Query);

        Assert.True(q.Columns[0].Strict);
        Assert.Equal("42", q.Columns[0].Default);
    }

    [Fact]
    public void StringDefaultValue()
    {
        var result = QueryParser.Parse("create table t (name string default 'unknown')");
        var q = Assert.IsType<CreateTableQuery>(result.Query);

        Assert.Equal("unknown", q.Columns[0].Default);
    }

    [Fact]
    public void NegativeDefaultValue()
    {
        var result = QueryParser.Parse("create table t (temp sshort default -10)");
        var q = Assert.IsType<CreateTableQuery>(result.Query);

        Assert.Equal("-10", q.Columns[0].Default);
    }

    [Fact]
    public void FloatDefaultValue()
    {
        var result = QueryParser.Parse("create table t (rate double default 0.5)");
        var q = Assert.IsType<CreateTableQuery>(result.Query);

        Assert.Equal("0.5", q.Columns[0].Default);
    }

    [Fact]
    public void CaseInsensitive()
    {
        var result = QueryParser.Parse("CREATE TABLE Users (Name STRING, Age UBYTE)");

        Assert.True(result.Success);
        var q = Assert.IsType<CreateTableQuery>(result.Query);
        Assert.Equal("users", q.Table);
        Assert.Equal("name", q.Columns[0].Name);
        Assert.Equal(ColumnType.String, q.Columns[0].Type);
    }

    // ── All 14 types ─────────────────────────────────────────

    [Theory]
    [InlineData("string", (int)ColumnType.String, 255)]
    [InlineData("sbyte", (int)ColumnType.SByte, 1)]
    [InlineData("ubyte", (int)ColumnType.UByte, 1)]
    [InlineData("sshort", (int)ColumnType.SShort, 2)]
    [InlineData("ushort", (int)ColumnType.UShort, 2)]
    [InlineData("sint", (int)ColumnType.SInt, 4)]
    [InlineData("uint", (int)ColumnType.UInt, 4)]
    [InlineData("slong", (int)ColumnType.SLong, 8)]
    [InlineData("ulong", (int)ColumnType.ULong, 8)]
    [InlineData("float", (int)ColumnType.Float, 4)]
    [InlineData("double", (int)ColumnType.Double, 8)]
    [InlineData("bool", (int)ColumnType.Bool, 1)]
    [InlineData("date", (int)ColumnType.Date, 4)]
    [InlineData("time", (int)ColumnType.Time, 8)]
    [InlineData("datetime", (int)ColumnType.DateTime, 8)]
    public void AllColumnTypes(string typeName, int expectedTypeInt, int expectedSize)
    {
        var expectedType = (ColumnType)expectedTypeInt;
        var result = QueryParser.Parse($"create table t (col {typeName})");

        Assert.True(result.Success);
        var q = Assert.IsType<CreateTableQuery>(result.Query);
        Assert.Equal(expectedType, q.Columns[0].Type);
        Assert.Equal(expectedSize, q.Columns[0].Size);
    }

    // ── Entry size ───────────────────────────────────────────

    [Fact]
    public void EntrySize_IsSizePlusFlagByte()
    {
        var result = QueryParser.Parse("create table t (name string 100, age ubyte)");
        var q = Assert.IsType<CreateTableQuery>(result.Query);

        Assert.Equal(101, q.Columns[0].EntrySize); // 1 flag + 100
        Assert.Equal(2, q.Columns[1].EntrySize);   // 1 flag + 1
    }

    // ── Error cases ──────────────────────────────────────────

    [Fact]
    public void MissingTableName_Error()
    {
        var result = QueryParser.Parse("create table");
        Assert.False(result.Success);
        Assert.Contains("expected table name", result.Errors![0].Message);
    }

    [Fact]
    public void MissingColumnType_Error()
    {
        var result = QueryParser.Parse("create table t (name)");
        Assert.False(result.Success);
        Assert.Contains("expected column type", result.Errors![0].Message);
    }

    [Fact]
    public void ReservedColumnName_Id_Error()
    {
        var result = QueryParser.Parse("create table t (id string)");
        Assert.False(result.Success);
        Assert.Contains("'id' is reserved", result.Errors![0].Message);
    }

    [Fact]
    public void InvalidTableName_Error()
    {
        var result = QueryParser.Parse("create table 123bad");
        Assert.False(result.Success);
        Assert.Contains("expected table name", result.Errors![0].Message);
    }

    [Fact]
    public void TypeNameAsTableName_Error()
    {
        var result = QueryParser.Parse("create table string");
        Assert.False(result.Success);
        Assert.Contains("expected table name", result.Errors![0].Message);
    }

    [Fact]
    public void MissingClosingParen_Error()
    {
        var result = QueryParser.Parse("create table t (name string");
        Assert.False(result.Success);
    }

    [Fact]
    public void MissingDefaultValue_Error()
    {
        var result = QueryParser.Parse("create table t (name string default)");
        Assert.False(result.Success);
        Assert.Contains("expected default value", result.Errors![0].Message);
    }
}

namespace SproutDB.Core.Tests;

public class WhereTests : IDisposable
{
    private readonly string _tempDir;
    private readonly SproutEngine _engine;

    public WhereTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), $"sproutdb-test-{Guid.NewGuid()}");
        _engine = new SproutEngine(_tempDir);
        _engine.Execute("create database", "testdb");
        _engine.Execute(
            "create table users (name string 100, age ubyte, score sint, rating double)",
            "testdb");

        // Seed: Alice(28, 85, 4.5), Bob(35, 92, 3.8), Charlie(22, 70, 4.9), Diana(28, 88, 4.5)
        _engine.Execute("upsert users {name: 'Alice', age: 28, score: 85, rating: 4.5}", "testdb");
        _engine.Execute("upsert users {name: 'Bob', age: 35, score: 92, rating: 3.8}", "testdb");
        _engine.Execute("upsert users {name: 'Charlie', age: 22, score: 70, rating: 4.9}", "testdb");
        _engine.Execute("upsert users {name: 'Diana', age: 28, score: 88, rating: 4.5}", "testdb");
    }

    public void Dispose()
    {
        _engine.Dispose();
        if (Directory.Exists(_tempDir))
            Directory.Delete(_tempDir, true);
    }

    // ── Equal (=) ─────────────────────────────────────────────

    [Fact]
    public void Where_Equal_String()
    {
        var r = _engine.Execute("get users where name = 'Alice'", "testdb");

        Assert.Equal(SproutOperation.Get, r.Operation);
        Assert.Equal(1, r.Affected);
        Assert.Equal("Alice", r.Data![0]["name"]);
    }

    [Fact]
    public void Where_Equal_Integer()
    {
        var r = _engine.Execute("get users where age = 28", "testdb");

        Assert.Equal(2, r.Affected); // Alice + Diana
        Assert.All(r.Data!, row => Assert.Equal((byte)28, row["age"]));
    }

    [Fact]
    public void Where_Equal_NoMatch()
    {
        var r = _engine.Execute("get users where age = 99", "testdb");

        Assert.Equal(0, r.Affected);
        Assert.Empty(r.Data!);
    }

    // ── Not Equal (!=) ────────────────────────────────────────

    [Fact]
    public void Where_NotEqual()
    {
        var r = _engine.Execute("get users where age != 28", "testdb");

        Assert.Equal(2, r.Affected); // Bob + Charlie
        Assert.All(r.Data!, row => Assert.NotEqual((byte)28, row["age"]));
    }

    // ── Greater Than (>) ──────────────────────────────────────

    [Fact]
    public void Where_GreaterThan()
    {
        var r = _engine.Execute("get users where age > 28", "testdb");

        Assert.Equal(1, r.Affected); // Bob(35)
        Assert.Equal("Bob", r.Data![0]["name"]);
    }

    [Fact]
    public void Where_GreaterThan_NoMatch()
    {
        var r = _engine.Execute("get users where age > 100", "testdb");

        Assert.Equal(0, r.Affected);
    }

    // ── Greater Than Or Equal (>=) ────────────────────────────

    [Fact]
    public void Where_GreaterThanOrEqual()
    {
        var r = _engine.Execute("get users where age >= 28", "testdb");

        Assert.Equal(3, r.Affected); // Alice, Bob, Diana
    }

    // ── Less Than (<) ─────────────────────────────────────────

    [Fact]
    public void Where_LessThan()
    {
        var r = _engine.Execute("get users where age < 28", "testdb");

        Assert.Equal(1, r.Affected); // Charlie(22)
        Assert.Equal("Charlie", r.Data![0]["name"]);
    }

    // ── Less Than Or Equal (<=) ───────────────────────────────

    [Fact]
    public void Where_LessThanOrEqual()
    {
        var r = _engine.Execute("get users where age <= 28", "testdb");

        Assert.Equal(3, r.Affected); // Alice, Charlie, Diana
    }

    // ── Signed integer ────────────────────────────────────────

    [Fact]
    public void Where_SignedInt_GreaterThan()
    {
        var r = _engine.Execute("get users where score > 85", "testdb");

        Assert.Equal(2, r.Affected); // Bob(92), Diana(88)
    }

    [Fact]
    public void Where_SignedInt_Negative()
    {
        _engine.Execute("create table temps (value sint)", "testdb");
        _engine.Execute("upsert temps {value: -10}", "testdb");
        _engine.Execute("upsert temps {value: 5}", "testdb");
        _engine.Execute("upsert temps {value: -3}", "testdb");

        var r = _engine.Execute("get temps where value > -5", "testdb");

        Assert.Equal(2, r.Affected); // 5, -3
    }

    // ── Double ────────────────────────────────────────────────

    [Fact]
    public void Where_Double_GreaterThan()
    {
        var r = _engine.Execute("get users where rating > 4.0", "testdb");

        Assert.Equal(3, r.Affected); // Alice(4.5), Charlie(4.9), Diana(4.5)
    }

    [Fact]
    public void Where_Double_Equal()
    {
        var r = _engine.Execute("get users where rating = 4.5", "testdb");

        Assert.Equal(2, r.Affected); // Alice + Diana
    }

    // ── String comparison ─────────────────────────────────────

    [Fact]
    public void Where_String_GreaterThan()
    {
        var r = _engine.Execute("get users where name > 'Bob'", "testdb");

        Assert.Equal(2, r.Affected); // Charlie, Diana
    }

    [Fact]
    public void Where_String_LessThan()
    {
        var r = _engine.Execute("get users where name < 'Bob'", "testdb");

        Assert.Equal(1, r.Affected); // Alice
    }

    // ── ID filter ─────────────────────────────────────────────

    [Fact]
    public void Where_Id_Equal()
    {
        var r = _engine.Execute("get users where _id = 2", "testdb");

        Assert.Equal(1, r.Affected);
        Assert.Equal("Bob", r.Data![0]["name"]);
    }

    [Fact]
    public void Where_Id_GreaterThan()
    {
        var r = _engine.Execute("get users where _id > 2", "testdb");

        Assert.Equal(2, r.Affected); // Charlie(3), Diana(4)
    }

    // ── With select ───────────────────────────────────────────

    [Fact]
    public void Where_WithSelect()
    {
        var r = _engine.Execute("get users select name where age > 30", "testdb");

        Assert.Equal(1, r.Affected);
        Assert.Single(r.Data![0]); // only name
        Assert.Equal("Bob", r.Data[0]["name"]);
    }

    // ── Bool filter ───────────────────────────────────────────

    [Fact]
    public void Where_Bool()
    {
        _engine.Execute("create table flags (active bool default true)", "testdb");
        _engine.Execute("upsert flags {active: true}", "testdb");
        _engine.Execute("upsert flags {active: false}", "testdb");
        _engine.Execute("upsert flags {active: true}", "testdb");

        var r = _engine.Execute("get flags where active = true", "testdb");

        Assert.Equal(2, r.Affected);
    }

    // ── Null handling ─────────────────────────────────────────

    [Fact]
    public void Where_NullValues_Excluded()
    {
        _engine.Execute("upsert users {name: 'Eve'}", "testdb"); // age is null

        var r = _engine.Execute("get users where age > 0", "testdb");

        // Eve excluded because null never matches
        Assert.Equal(4, r.Affected); // original 4
    }

    // ── Error cases ───────────────────────────────────────────

    [Fact]
    public void Where_UnknownColumn_Error()
    {
        var r = _engine.Execute("get users where missing = 1", "testdb");

        Assert.Equal(SproutOperation.Error, r.Operation);
        Assert.Equal("UNKNOWN_COLUMN", r.Errors![0].Code);
    }

    [Fact]
    public void Where_CaseInsensitive()
    {
        var r = _engine.Execute("GET users WHERE age = 28", "testdb");

        Assert.Equal(SproutOperation.Get, r.Operation);
        Assert.Equal(2, r.Affected);
    }

    // ── Contains ──────────────────────────────────────────────

    [Fact]
    public void Where_Contains_Match()
    {
        var r = _engine.Execute("get users where name contains 'li'", "testdb");

        Assert.Equal(SproutOperation.Get, r.Operation);
        Assert.Equal(2, r.Affected); // Alice, Charlie
    }

    [Fact]
    public void Where_Contains_NoMatch()
    {
        var r = _engine.Execute("get users where name contains 'xyz'", "testdb");

        Assert.Equal(0, r.Affected);
    }

    [Fact]
    public void Where_Contains_FullValue()
    {
        var r = _engine.Execute("get users where name contains 'Bob'", "testdb");

        Assert.Equal(1, r.Affected);
        Assert.Equal("Bob", r.Data![0]["name"]);
    }

    // ── StartsWith ────────────────────────────────────────────

    [Fact]
    public void Where_StartsWith_Match()
    {
        var r = _engine.Execute("get users where name starts 'Al'", "testdb");

        Assert.Equal(1, r.Affected);
        Assert.Equal("Alice", r.Data![0]["name"]);
    }

    [Fact]
    public void Where_StartsWith_NoMatch()
    {
        var r = _engine.Execute("get users where name starts 'Zz'", "testdb");

        Assert.Equal(0, r.Affected);
    }

    // ── EndsWith ──────────────────────────────────────────────

    [Fact]
    public void Where_EndsWith_Match()
    {
        var r = _engine.Execute("get users where name ends 'ob'", "testdb");

        Assert.Equal(1, r.Affected);
        Assert.Equal("Bob", r.Data![0]["name"]);
    }

    [Fact]
    public void Where_EndsWith_NoMatch()
    {
        var r = _engine.Execute("get users where name ends 'zzz'", "testdb");

        Assert.Equal(0, r.Affected);
    }

    // ── Null handling for string ops ──────────────────────────

    [Fact]
    public void Where_Contains_NullValues_Excluded()
    {
        _engine.Execute("upsert users {age: 30}", "testdb"); // name is null

        var r = _engine.Execute("get users where name contains 'li'", "testdb");

        Assert.Equal(2, r.Affected); // Alice, Charlie — null row excluded
    }

    // ── TYPE_MISMATCH errors ─────────────────────────────────

    [Fact]
    public void Where_Contains_OnNumericColumn_Error()
    {
        var r = _engine.Execute("get users where age contains '28'", "testdb");

        Assert.Equal(SproutOperation.Error, r.Operation);
        Assert.Equal("TYPE_MISMATCH", r.Errors![0].Code);
        Assert.Contains("string or array columns", r.Errors[0].Message);
    }

    [Fact]
    public void Where_Starts_OnId_Error()
    {
        var r = _engine.Execute("get users where _id starts '1'", "testdb");

        Assert.Equal(SproutOperation.Error, r.Operation);
        Assert.Equal("TYPE_MISMATCH", r.Errors![0].Code);
        Assert.Contains("_id", r.Errors[0].Message);
    }

    // ── With Select ──────────────────────────────────────────

    [Fact]
    public void Where_Contains_WithSelect()
    {
        var r = _engine.Execute("get users select name where name contains 'ob'", "testdb");

        Assert.Equal(1, r.Affected);
        Assert.Single(r.Data![0]); // only name
        Assert.Equal("Bob", r.Data[0]["name"]);
    }

    // ── AND ─────────────────────────────────────────────────

    [Fact]
    public void Where_And_Match()
    {
        // Alice(28, 85), Diana(28, 88) → age=28 and score>85 → Diana
        var r = _engine.Execute("get users where age = 28 and score > 85", "testdb");

        Assert.Equal(SproutOperation.Get, r.Operation);
        Assert.Equal(1, r.Affected);
        Assert.Equal("Diana", r.Data![0]["name"]);
    }

    [Fact]
    public void Where_And_PartialMatch_Excluded()
    {
        // age=28 and name='Bob' → nobody (Bob is 35)
        var r = _engine.Execute("get users where age = 28 and name = 'Bob'", "testdb");

        Assert.Equal(0, r.Affected);
    }

    // ── OR ──────────────────────────────────────────────────

    [Fact]
    public void Where_Or_Match()
    {
        // Alice(28) or Bob(35) → age=28 or age=35 → 3 (Alice, Bob, Diana)
        var r = _engine.Execute("get users where age = 35 or age = 22", "testdb");

        Assert.Equal(2, r.Affected); // Bob, Charlie
    }

    [Fact]
    public void Where_Or_NoMatch()
    {
        var r = _engine.Execute("get users where age = 99 or age = 100", "testdb");

        Assert.Equal(0, r.Affected);
    }

    // ── NOT ─────────────────────────────────────────────────

    [Fact]
    public void Where_Not()
    {
        // not age=28 → Bob(35), Charlie(22)
        var r = _engine.Execute("get users where not age = 28", "testdb");

        Assert.Equal(2, r.Affected);
        Assert.All(r.Data!, row => Assert.NotEqual((byte)28, row["age"]));
    }

    [Fact]
    public void Where_Not_With_And()
    {
        // not age=28 and name!='Bob' → Charlie (NOT only binds to first)
        var r = _engine.Execute("get users where not age = 28 and name != 'Bob'", "testdb");

        Assert.Equal(1, r.Affected);
        Assert.Equal("Charlie", r.Data![0]["name"]);
    }

    // ── IS NULL ─────────────────────────────────────────────

    [Fact]
    public void Where_IsNull()
    {
        _engine.Execute("upsert users {name: 'Eve'}", "testdb"); // age is null

        var r = _engine.Execute("get users where age is null", "testdb");

        Assert.Equal(1, r.Affected);
        Assert.Equal("Eve", r.Data![0]["name"]);
    }

    [Fact]
    public void Where_IsNotNull()
    {
        _engine.Execute("upsert users {name: 'Eve'}", "testdb"); // age is null

        var r = _engine.Execute("get users where age is not null", "testdb");

        Assert.Equal(4, r.Affected); // original 4
    }

    [Fact]
    public void Where_IsNull_OnId_AlwaysEmpty()
    {
        var r = _engine.Execute("get users where _id is null", "testdb");

        Assert.Equal(0, r.Affected);
    }

    // ── Kombination: IS NOT NULL + AND ──────────────────────

    [Fact]
    public void Where_IsNotNull_And_Compare()
    {
        _engine.Execute("upsert users {name: 'Eve'}", "testdb"); // age is null

        var r = _engine.Execute("get users where name is not null and age > 25", "testdb");

        Assert.Equal(3, r.Affected); // Alice(28), Bob(35), Diana(28)
    }

    // ── Nested AND+OR (precedence) ──────────────────────────

    [Fact]
    public void Where_Nested_And_Or_Precedence()
    {
        // age > 30 or name = 'Alice' and age = 28
        // → OR(age>30, AND(name='Alice', age=28))
        // → Bob(35) + Alice(28) = 2
        var r = _engine.Execute("get users where age > 30 or name = 'Alice' and age = 28", "testdb");

        Assert.Equal(2, r.Affected);
        var names = r.Data!.Select(d => (string)d["name"]!).OrderBy(n => n).ToList();
        Assert.Equal(["Alice", "Bob"], names);
    }

    // ── Nested NOT+AND ──────────────────────────────────────

    [Fact]
    public void Where_Nested_Not_And()
    {
        // not age = 28 and name != 'Bob' → Charlie
        var r = _engine.Execute("get users where not age = 28 and name != 'Bob'", "testdb");

        Assert.Equal(1, r.Affected);
        Assert.Equal("Charlie", r.Data![0]["name"]);
    }

    // ── Three AND ───────────────────────────────────────────

    [Fact]
    public void Where_Three_And()
    {
        // age >= 22 and age <= 28 and name != 'Diana' → Alice(28), Charlie(22)
        var r = _engine.Execute("get users where age >= 22 and age <= 28 and name != 'Diana'", "testdb");

        Assert.Equal(2, r.Affected);
        var names = r.Data!.Select(d => (string)d["name"]!).OrderBy(n => n).ToList();
        Assert.Equal(["Alice", "Charlie"], names);
    }

    // ── OR mit NOT ──────────────────────────────────────────

    [Fact]
    public void Where_Or_With_Not()
    {
        // not age = 28 or name = 'Diana' → Bob(35), Charlie(22), Diana(28)
        var r = _engine.Execute("get users where not age = 28 or name = 'Diana'", "testdb");

        Assert.Equal(3, r.Affected);
        var names = r.Data!.Select(d => (string)d["name"]!).OrderBy(n => n).ToList();
        Assert.Equal(["Bob", "Charlie", "Diana"], names);
    }

    // ── IN ───────────────────────────────────────────────────

    [Fact]
    public void Where_In_Match()
    {
        // age in [28, 35] → Alice(28), Bob(35), Diana(28)
        var r = _engine.Execute("get users where age in [28, 35]", "testdb");

        Assert.Equal(SproutOperation.Get, r.Operation);
        Assert.Equal(3, r.Affected);
        var names = r.Data!.Select(d => (string)d["name"]!).OrderBy(n => n).ToList();
        Assert.Equal(["Alice", "Bob", "Diana"], names);
    }

    [Fact]
    public void Where_In_NoMatch()
    {
        var r = _engine.Execute("get users where age in [99, 100]", "testdb");

        Assert.Equal(0, r.Affected);
    }

    [Fact]
    public void Where_In_String()
    {
        var r = _engine.Execute("get users where name in ['Alice', 'Bob']", "testdb");

        Assert.Equal(2, r.Affected);
        var names = r.Data!.Select(d => (string)d["name"]!).OrderBy(n => n).ToList();
        Assert.Equal(["Alice", "Bob"], names);
    }

    [Fact]
    public void Where_NotIn()
    {
        // age not in [28, 35] → Charlie(22)
        var r = _engine.Execute("get users where age not in [28, 35]", "testdb");

        Assert.Equal(1, r.Affected);
        Assert.Equal("Charlie", r.Data![0]["name"]);
    }

    [Fact]
    public void Where_In_Id()
    {
        var r = _engine.Execute("get users where _id in [1, 3]", "testdb");

        Assert.Equal(2, r.Affected);
        var names = r.Data!.Select(d => (string)d["name"]!).OrderBy(n => n).ToList();
        Assert.Equal(["Alice", "Charlie"], names);
    }

    [Fact]
    public void Where_In_NullExcluded()
    {
        _engine.Execute("upsert users {name: 'Eve'}", "testdb"); // age is null

        var r = _engine.Execute("get users where age in [28, 35]", "testdb");

        Assert.Equal(3, r.Affected); // Alice, Bob, Diana — Eve excluded
    }

    [Fact]
    public void Where_In_With_And()
    {
        // age in [28] and name = 'Alice' → Alice only
        var r = _engine.Execute("get users where age in [28] and name = 'Alice'", "testdb");

        Assert.Equal(1, r.Affected);
        Assert.Equal("Alice", r.Data![0]["name"]);
    }

    // ── DateTime comparison (#046) ───────────────────────────

    [Fact]
    public void Where_DateTime_GreaterThan()
    {
        _engine.Execute("create table events (title string 100, created datetime)", "testdb");
        _engine.Execute("upsert events {title: 'Early', created: '2024-06-15 10:00:00'}", "testdb");
        _engine.Execute("upsert events {title: 'Mid', created: '2025-01-01 14:30:00'}", "testdb");
        _engine.Execute("upsert events {title: 'Late', created: '2025-07-20 08:00:00'}", "testdb");

        var r = _engine.Execute("get events where created > '2025-01-01 00:00:00'", "testdb");

        Assert.Equal(2, r.Affected); // Mid, Late
    }

    [Fact]
    public void Where_DateTime_Equal()
    {
        _engine.Execute("create table logs (msg string 100, ts datetime)", "testdb");
        _engine.Execute("upsert logs {msg: 'a', ts: '2025-03-15 12:00:00'}", "testdb");
        _engine.Execute("upsert logs {msg: 'b', ts: '2025-03-15 13:00:00'}", "testdb");

        var r = _engine.Execute("get logs where ts = '2025-03-15 12:00:00'", "testdb");

        Assert.Equal(1, r.Affected);
        Assert.Equal("a", r.Data![0]["msg"]);
    }

    [Fact]
    public void Where_DateTime_LessThanOrEqual()
    {
        _engine.Execute("create table posts (title string 100, published datetime)", "testdb");
        _engine.Execute("upsert posts {title: 'Old', published: '2024-01-01 00:00:00'}", "testdb");
        _engine.Execute("upsert posts {title: 'New', published: '2025-06-01 00:00:00'}", "testdb");

        var r = _engine.Execute("get posts where published <= '2024-12-31 23:59:59'", "testdb");

        Assert.Equal(1, r.Affected);
        Assert.Equal("Old", r.Data![0]["title"]);
    }

    // ── Date comparison (#047) ───────────────────────────────

    [Fact]
    public void Where_Date_GreaterThan()
    {
        _engine.Execute("create table people (name string 100, birthday date)", "testdb");
        _engine.Execute("upsert people {name: 'Young', birthday: '2005-08-20'}", "testdb");
        _engine.Execute("upsert people {name: 'Old', birthday: '1990-03-10'}", "testdb");
        _engine.Execute("upsert people {name: 'Mid', birthday: '2000-01-01'}", "testdb");

        var r = _engine.Execute("get people where birthday > '2000-01-01'", "testdb");

        Assert.Equal(1, r.Affected); // Young
        Assert.Equal("Young", r.Data![0]["name"]);
    }

    [Fact]
    public void Where_Date_Equal()
    {
        _engine.Execute("create table holidays (name string 100, day date)", "testdb");
        _engine.Execute("upsert holidays {name: 'NewYear', day: '2025-01-01'}", "testdb");
        _engine.Execute("upsert holidays {name: 'Xmas', day: '2025-12-25'}", "testdb");

        var r = _engine.Execute("get holidays where day = '2025-12-25'", "testdb");

        Assert.Equal(1, r.Affected);
        Assert.Equal("Xmas", r.Data![0]["name"]);
    }

    [Fact]
    public void Where_Date_LessThan()
    {
        _engine.Execute("create table deadlines (task string 100, due date)", "testdb");
        _engine.Execute("upsert deadlines {task: 'Past', due: '2024-06-01'}", "testdb");
        _engine.Execute("upsert deadlines {task: 'Future', due: '2026-01-01'}", "testdb");

        var r = _engine.Execute("get deadlines where due < '2025-01-01'", "testdb");

        Assert.Equal(1, r.Affected);
        Assert.Equal("Past", r.Data![0]["task"]);
    }

    // ── Time comparison (#048) ───────────────────────────────

    [Fact]
    public void Where_Time_GreaterThan()
    {
        _engine.Execute("create table shifts (worker string 100, start time)", "testdb");
        _engine.Execute("upsert shifts {worker: 'Morning', start: '06:00:00'}", "testdb");
        _engine.Execute("upsert shifts {worker: 'Day', start: '09:00:00'}", "testdb");
        _engine.Execute("upsert shifts {worker: 'Night', start: '22:00:00'}", "testdb");

        var r = _engine.Execute("get shifts where start > '08:00:00'", "testdb");

        Assert.Equal(2, r.Affected); // Day, Night
    }

    [Fact]
    public void Where_Time_Equal()
    {
        _engine.Execute("create table alarms (label string 100, ring time)", "testdb");
        _engine.Execute("upsert alarms {label: 'Wake', ring: '07:00:00'}", "testdb");
        _engine.Execute("upsert alarms {label: 'Lunch', ring: '12:00:00'}", "testdb");

        var r = _engine.Execute("get alarms where ring = '07:00:00'", "testdb");

        Assert.Equal(1, r.Affected);
        Assert.Equal("Wake", r.Data![0]["label"]);
    }

    [Fact]
    public void Where_Time_LessThanOrEqual()
    {
        _engine.Execute("create table breaks (name string 100, at time)", "testdb");
        _engine.Execute("upsert breaks {name: 'Coffee', at: '10:30:00'}", "testdb");
        _engine.Execute("upsert breaks {name: 'Lunch', at: '12:00:00'}", "testdb");
        _engine.Execute("upsert breaks {name: 'Tea', at: '15:00:00'}", "testdb");

        var r = _engine.Execute("get breaks where at <= '12:00:00'", "testdb");

        Assert.Equal(2, r.Affected); // Coffee, Lunch
    }

    // ── Date/Time null handling ──────────────────────────────

    [Fact]
    public void Where_Date_NullValues_Excluded()
    {
        _engine.Execute("create table members (name string 100, joined date)", "testdb");
        _engine.Execute("upsert members {name: 'A', joined: '2025-01-01'}", "testdb");
        _engine.Execute("upsert members {name: 'B'}", "testdb"); // joined is null

        var r = _engine.Execute("get members where joined > '2024-01-01'", "testdb");

        Assert.Equal(1, r.Affected); // only A, B excluded
    }

    // ── Between – numeric (#042) ─────────────────────────────

    [Fact]
    public void Where_Between_Integer()
    {
        var r = _engine.Execute("get users where age between 25 and 30", "testdb");

        Assert.Equal(2, r.Affected); // Alice(28), Diana(28)
    }

    [Fact]
    public void Where_Between_Inclusive()
    {
        // boundary values included (like SQL)
        var r = _engine.Execute("get users where age between 22 and 28", "testdb");

        Assert.Equal(3, r.Affected); // Alice(28), Charlie(22), Diana(28)
    }

    [Fact]
    public void Where_Between_NoMatch()
    {
        var r = _engine.Execute("get users where age between 40 and 50", "testdb");

        Assert.Equal(0, r.Affected);
    }

    [Fact]
    public void Where_Between_Double()
    {
        var r = _engine.Execute("get users where rating between 4.0 and 4.6", "testdb");

        Assert.Equal(2, r.Affected); // Alice(4.5), Diana(4.5)
    }

    [Fact]
    public void Where_Between_SignedInt()
    {
        _engine.Execute("create table temps (value sint)", "testdb");
        _engine.Execute("upsert temps {value: -10}", "testdb");
        _engine.Execute("upsert temps {value: 5}", "testdb");
        _engine.Execute("upsert temps {value: -3}", "testdb");
        _engine.Execute("upsert temps {value: 20}", "testdb");

        var r = _engine.Execute("get temps where value between -5 and 10", "testdb");

        Assert.Equal(2, r.Affected); // -3, 5
    }

    [Fact]
    public void Where_Between_Id()
    {
        var r = _engine.Execute("get users where _id between 2 and 3", "testdb");

        Assert.Equal(2, r.Affected); // Bob(2), Charlie(3)
    }

    [Fact]
    public void Where_Between_NullExcluded()
    {
        _engine.Execute("upsert users {name: 'Eve'}", "testdb"); // age is null

        var r = _engine.Execute("get users where age between 20 and 40", "testdb");

        Assert.Equal(4, r.Affected); // original 4, Eve excluded
    }

    // ── Not Between (#043) ──────────────────────────────────

    [Fact]
    public void Where_NotBetween_Integer()
    {
        var r = _engine.Execute("get users where age not between 25 and 30", "testdb");

        Assert.Equal(2, r.Affected); // Bob(35), Charlie(22)
    }

    [Fact]
    public void Where_NotBetween_NullExcluded()
    {
        _engine.Execute("upsert users {name: 'Eve'}", "testdb"); // age is null

        var r = _engine.Execute("get users where age not between 25 and 30", "testdb");

        Assert.Equal(2, r.Affected); // Bob, Charlie — null excluded
    }

    // ── Between – DateTime (#049) ────────────────────────────

    [Fact]
    public void Where_Between_DateTime()
    {
        _engine.Execute("create table events2 (title string 100, created datetime)", "testdb");
        _engine.Execute("upsert events2 {title: 'A', created: '2024-06-15 10:00:00'}", "testdb");
        _engine.Execute("upsert events2 {title: 'B', created: '2025-03-01 14:30:00'}", "testdb");
        _engine.Execute("upsert events2 {title: 'C', created: '2025-07-20 08:00:00'}", "testdb");

        var r = _engine.Execute("get events2 where created between '2025-01-01 00:00:00' and '2025-06-01 00:00:00'", "testdb");

        Assert.Equal(1, r.Affected); // B
        Assert.Equal("B", r.Data![0]["title"]);
    }

    // ── Between – Date (#049A) ───────────────────────────────

    [Fact]
    public void Where_Between_Date()
    {
        _engine.Execute("create table people2 (name string 100, birthday date)", "testdb");
        _engine.Execute("upsert people2 {name: 'Young', birthday: '2005-08-20'}", "testdb");
        _engine.Execute("upsert people2 {name: 'Old', birthday: '1990-03-10'}", "testdb");
        _engine.Execute("upsert people2 {name: 'Mid', birthday: '2000-06-15'}", "testdb");

        var r = _engine.Execute("get people2 where birthday between '2000-01-01' and '2005-12-31'", "testdb");

        Assert.Equal(2, r.Affected); // Mid, Young
    }

    // ── Between – Time (#049B) ───────────────────────────────

    [Fact]
    public void Where_Between_Time()
    {
        _engine.Execute("create table shifts2 (worker string 100, start time)", "testdb");
        _engine.Execute("upsert shifts2 {worker: 'Early', start: '06:00:00'}", "testdb");
        _engine.Execute("upsert shifts2 {worker: 'Day', start: '09:00:00'}", "testdb");
        _engine.Execute("upsert shifts2 {worker: 'Late', start: '14:00:00'}", "testdb");
        _engine.Execute("upsert shifts2 {worker: 'Night', start: '22:00:00'}", "testdb");

        var r = _engine.Execute("get shifts2 where start between '08:00:00' and '17:00:00'", "testdb");

        Assert.Equal(2, r.Affected); // Day, Late
    }
}

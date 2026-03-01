namespace SproutDB.Core.Tests.Linq;

public class SproutTableTests : IDisposable
{
    private class User : ISproutEntity
    {
        public ulong Id { get; set; }
        public string? Name { get; set; }
        public string? Email { get; set; }
        public byte Age { get; set; }
        public bool Active { get; set; }
    }

    private readonly string _tempDir;
    private readonly SproutEngine _engine;
    private readonly ISproutDatabase _db;

    public SproutTableTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), $"sproutdb-test-{Guid.NewGuid()}");
        _engine = new SproutEngine(_tempDir);
        _db = _engine.GetOrCreateDatabase("testdb");
        _db.Query("create table users (name string 100, email string 320 strict, age ubyte, active bool default true)");
        _db.Query("upsert users {name: 'Alice', email: 'alice@test.com', age: 28, active: true}");
        _db.Query("upsert users {name: 'Bob', email: 'bob@test.com', age: 35, active: true}");
        _db.Query("upsert users {name: 'Charlie', email: 'charlie@test.com', age: 22, active: false}");
    }

    public void Dispose()
    {
        _engine.Dispose();
        if (Directory.Exists(_tempDir))
            Directory.Delete(_tempDir, true);
    }

    // ── ToList ──────────────────────────────────────────────────

    [Fact]
    public void ToList_All()
    {
        var users = _db.Table<User>("users").ToList();

        Assert.Equal(3, users.Count);
        Assert.All(users, u => Assert.True(u.Id > 0));
    }

    [Fact]
    public void Where_ToList()
    {
        var users = _db.Table<User>("users")
            .Where(u => u.Age > 25)
            .ToList();

        Assert.Equal(2, users.Count);
        Assert.Contains(users, u => u.Name == "Alice");
        Assert.Contains(users, u => u.Name == "Bob");
    }

    [Fact]
    public void Where_And_ToList()
    {
        var users = _db.Table<User>("users")
            .Where(u => u.Age > 20 && u.Active == true)
            .ToList();

        Assert.Equal(2, users.Count);
        Assert.DoesNotContain(users, u => u.Name == "Charlie");
    }

    [Fact]
    public void Where_Or_ToList()
    {
        var users = _db.Table<User>("users")
            .Where(u => u.Age < 25 || u.Age > 30)
            .ToList();

        Assert.Equal(2, users.Count);
        Assert.Contains(users, u => u.Name == "Charlie");
        Assert.Contains(users, u => u.Name == "Bob");
    }

    [Fact]
    public void Where_StringContains_ToList()
    {
        var users = _db.Table<User>("users")
            .Where(u => u.Name.Contains("li"))
            .ToList();

        Assert.Equal(2, users.Count);
        Assert.Contains(users, u => u.Name == "Alice");
        Assert.Contains(users, u => u.Name == "Charlie");
    }

    [Fact]
    public void Where_CapturedVariable_ToList()
    {
        byte minAge = 25;
        var users = _db.Table<User>("users")
            .Where(u => u.Age > minAge)
            .ToList();

        Assert.Equal(2, users.Count);
    }

    // ── Select ──────────────────────────────────────────────────

    [Fact]
    public void Select_ToList()
    {
        var response = _db.Table<User>("users")
            .Select(u => new { u.Name, u.Age })
            .Run();

        Assert.NotNull(response.Data);
        Assert.Equal(3, response.Data.Count);
        // Selected columns only
        Assert.True(response.Data[0].ContainsKey("name"));
        Assert.True(response.Data[0].ContainsKey("age"));
    }

    // ── OrderBy ─────────────────────────────────────────────────

    [Fact]
    public void OrderBy_ToList()
    {
        var users = _db.Table<User>("users")
            .OrderBy(u => u.Age)
            .ToList();

        Assert.Equal("Charlie", users[0].Name);
        Assert.Equal("Alice", users[1].Name);
        Assert.Equal("Bob", users[2].Name);
    }

    [Fact]
    public void OrderByDescending_ToList()
    {
        var users = _db.Table<User>("users")
            .OrderByDescending(u => u.Age)
            .ToList();

        Assert.Equal("Bob", users[0].Name);
        Assert.Equal("Alice", users[1].Name);
        Assert.Equal("Charlie", users[2].Name);
    }

    // ── Take ────────────────────────────────────────────────────

    [Fact]
    public void Take_ToList()
    {
        var users = _db.Table<User>("users")
            .Take(2)
            .ToList();

        Assert.Equal(2, users.Count);
    }

    // ── FirstOrDefault ──────────────────────────────────────────

    [Fact]
    public void FirstOrDefault_Found()
    {
        var user = _db.Table<User>("users")
            .FirstOrDefault(u => u.Id == 1);

        Assert.NotNull(user);
        Assert.Equal("Alice", user.Name);
    }

    [Fact]
    public void FirstOrDefault_NotFound()
    {
        var user = _db.Table<User>("users")
            .FirstOrDefault(u => u.Id == 999);

        Assert.Null(user);
    }

    // ── Count ───────────────────────────────────────────────────

    [Fact]
    public void Count_All()
    {
        var count = _db.Table<User>("users").Count();
        Assert.Equal(3, count);
    }

    [Fact]
    public void Count_WithWhere()
    {
        var count = _db.Table<User>("users")
            .Where(u => u.Age > 22)
            .Count();

        Assert.Equal(2, count);
    }

    // ── Run ─────────────────────────────────────────────────────

    [Fact]
    public void Run_ReturnsRawResponse()
    {
        var response = _db.Table<User>("users")
            .Where(u => u.Age > 30)
            .Run();

        Assert.Equal(SproutOperation.Get, response.Operation);
        Assert.NotNull(response.Data);
        Assert.Single(response.Data);
    }

    // ── Upsert ──────────────────────────────────────────────────

    [Fact]
    public void Upsert_Insert()
    {
        var users = _db.Table<User>("users");
        var response = users.Upsert(new User { Name = "Eve", Email = "eve@test.com", Age = 25, Active = true });

        Assert.Equal(SproutOperation.Upsert, response.Operation);
        Assert.Null(response.Errors);

        var all = users.ToList();
        Assert.Equal(4, all.Count);
        Assert.Contains(all, u => u.Name == "Eve");
    }

    [Fact]
    public void Upsert_Update()
    {
        var users = _db.Table<User>("users");
        var response = users.Upsert(new User { Id = 1, Name = "Alice Updated", Email = "alice@test.com", Age = 29, Active = true });

        Assert.Equal(SproutOperation.Upsert, response.Operation);

        var alice = users.FirstOrDefault(u => u.Id == 1);
        Assert.NotNull(alice);
        Assert.Equal("Alice Updated", alice.Name);
        Assert.Equal(29, alice.Age);
    }

    [Fact]
    public void Upsert_Anonymous_Partial()
    {
        var users = _db.Table<User>("users");
        users.Upsert((object)new { Id = 1ul, Age = (byte)30 });

        var alice = users.FirstOrDefault(u => u.Id == 1);
        Assert.NotNull(alice);
        Assert.Equal(30, alice.Age);
    }

    [Fact]
    public void Upsert_WithOnClause()
    {
        var users = _db.Table<User>("users");
        var response = users.Upsert(
            new User { Name = "Alice New", Email = "alice@test.com", Age = 29, Active = true },
            on: u => u.Email);

        Assert.Equal(SproutOperation.Upsert, response.Operation);

        var alice = users.FirstOrDefault(u => u.Email == "alice@test.com");
        Assert.NotNull(alice);
        Assert.Equal("Alice New", alice.Name);
    }

    [Fact]
    public void Upsert_Bulk()
    {
        var users = _db.Table<User>("users");
        var newUsers = new[]
        {
            new User { Name = "Dave", Email = "dave@test.com", Age = 40, Active = true },
            new User { Name = "Eve", Email = "eve@test.com", Age = 25, Active = true },
        };

        var response = users.Upsert(newUsers);
        Assert.Equal(SproutOperation.Upsert, response.Operation);

        var all = users.ToList();
        Assert.Equal(5, all.Count);
    }

    [Fact]
    public void Upsert_Bulk_WithOnClause()
    {
        var users = _db.Table<User>("users");
        var updates = new[]
        {
            new User { Name = "Alice V2", Email = "alice@test.com", Age = 29, Active = true },
            new User { Name = "Bob V2", Email = "bob@test.com", Age = 36, Active = true },
        };

        users.Upsert(updates, on: u => u.Email);

        var alice = users.FirstOrDefault(u => u.Email == "alice@test.com");
        Assert.NotNull(alice);
        Assert.Equal("Alice V2", alice.Name);
    }

    // ── Delete ──────────────────────────────────────────────────

    [Fact]
    public void Delete_ByPredicate()
    {
        var users = _db.Table<User>("users");
        var response = users.Delete(u => u.Active == false);

        Assert.Equal(SproutOperation.Delete, response.Operation);
        Assert.Equal(1, response.Affected);

        var remaining = users.ToList();
        Assert.Equal(2, remaining.Count);
        Assert.DoesNotContain(remaining, u => u.Name == "Charlie");
    }

    // ── Error handling ──────────────────────────────────────────

    [Fact]
    public void ToList_UnknownTable_Throws()
    {
        Assert.Throws<SproutQueryException>(() =>
            _db.Table<User>("nonexistent").ToList());
    }

    [Fact]
    public void Run_UnknownTable_ReturnsErrors()
    {
        var response = _db.Table<User>("nonexistent").Run();

        Assert.Equal(SproutOperation.Error, response.Operation);
        Assert.NotNull(response.Errors);
    }
}

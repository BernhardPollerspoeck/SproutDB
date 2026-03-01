using SproutDB.Core.Linq;

namespace SproutDB.Core.Tests.Linq;

public class TypeMapperTests
{
    private class TestUser : ISproutEntity
    {
        public ulong Id { get; set; }
        public string? Name { get; set; }
        public byte Age { get; set; }
        public bool Active { get; set; }
    }

    // ── ToColumnName ────────────────────────────────────────────

    [Fact]
    public void ToColumnName_Simple() => Assert.Equal("age", TypeMapper.ToColumnName("Age"));

    [Fact]
    public void ToColumnName_Id() => Assert.Equal("_id", TypeMapper.ToColumnName("Id"));

    [Fact]
    public void ToColumnName_MultiWord() => Assert.Equal("firstname", TypeMapper.ToColumnName("FirstName"));

    // ── Deserialize ─────────────────────────────────────────────

    [Fact]
    public void Deserialize_FullRecord()
    {
        var row = new Dictionary<string, object?>
        {
            ["_id"] = 1ul,
            ["name"] = "Alice",
            ["age"] = (byte)28,
            ["active"] = true,
        };

        var user = TypeMapper.Deserialize<TestUser>(row);

        Assert.Equal(1ul, user.Id);
        Assert.Equal("Alice", user.Name);
        Assert.Equal(28, user.Age);
        Assert.True(user.Active);
    }

    [Fact]
    public void Deserialize_NullValues()
    {
        var row = new Dictionary<string, object?>
        {
            ["_id"] = 1ul,
            ["name"] = null,
            ["age"] = (byte)0,
            ["active"] = false,
        };

        var user = TypeMapper.Deserialize<TestUser>(row);

        Assert.Null(user.Name);
    }

    [Fact]
    public void Deserialize_MissingKeys()
    {
        var row = new Dictionary<string, object?> { ["_id"] = 1ul };

        var user = TypeMapper.Deserialize<TestUser>(row);

        Assert.Equal(1ul, user.Id);
        Assert.Null(user.Name);
        Assert.Equal(0, user.Age);
        Assert.False(user.Active);
    }

    // ── Serialize ───────────────────────────────────────────────

    [Fact]
    public void Serialize_FullRecord()
    {
        var user = new TestUser { Id = 1, Name = "Alice", Age = 28, Active = true };
        var result = TypeMapper.SerializeToUpsertFields(user);

        Assert.Contains("_id: 1", result);
        Assert.Contains("name: 'Alice'", result);
        Assert.Contains("age: 28", result);
        Assert.Contains("active: true", result);
    }

    [Fact]
    public void Serialize_SkipDefaultId()
    {
        var user = new TestUser { Name = "Bob", Age = 30 };
        var result = TypeMapper.SerializeToUpsertFields(user);

        Assert.DoesNotContain("_id", result);
        Assert.Contains("name: 'Bob'", result);
    }

    [Fact]
    public void Serialize_StringEscape()
    {
        var user = new TestUser { Name = "O'Brien" };
        var result = TypeMapper.SerializeToUpsertFields(user);

        Assert.Contains("O\\'Brien", result);
    }

    [Fact]
    public void Serialize_NullValue()
    {
        var user = new TestUser { Id = 1, Name = null };
        var result = TypeMapper.SerializeToUpsertFields(user);

        Assert.Contains("name: null", result);
    }
}

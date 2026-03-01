using System.Linq.Expressions;
using SproutDB.Core.Linq;

namespace SproutDB.Core.Tests.Linq;

public class ExpressionVisitorTests
{
    private class TestUser : ISproutEntity
    {
        public ulong Id { get; set; }
        public string? Name { get; set; }
        public byte Age { get; set; }
        public bool Active { get; set; }
    }

    // ── Where: comparison operators ─────────────────────────────

    [Fact]
    public void Where_Equal()
    {
        var result = SproutExpressionVisitor.ConvertWhere<TestUser>(u => u.Age == 28);
        Assert.Equal("age = 28", result);
    }

    [Fact]
    public void Where_NotEqual()
    {
        var result = SproutExpressionVisitor.ConvertWhere<TestUser>(u => u.Age != 28);
        Assert.Equal("age != 28", result);
    }

    [Fact]
    public void Where_GreaterThan()
    {
        var result = SproutExpressionVisitor.ConvertWhere<TestUser>(u => u.Age > 18);
        Assert.Equal("age > 18", result);
    }

    [Fact]
    public void Where_LessThan()
    {
        var result = SproutExpressionVisitor.ConvertWhere<TestUser>(u => u.Age < 30);
        Assert.Equal("age < 30", result);
    }

    [Fact]
    public void Where_GreaterThanOrEqual()
    {
        var result = SproutExpressionVisitor.ConvertWhere<TestUser>(u => u.Age >= 18);
        Assert.Equal("age >= 18", result);
    }

    [Fact]
    public void Where_LessThanOrEqual()
    {
        var result = SproutExpressionVisitor.ConvertWhere<TestUser>(u => u.Age <= 30);
        Assert.Equal("age <= 30", result);
    }

    // ── Where: logical operators ────────────────────────────────

    [Fact]
    public void Where_And()
    {
        var result = SproutExpressionVisitor.ConvertWhere<TestUser>(u => u.Age > 18 && u.Active == true);
        Assert.Equal("age > 18 and active = true", result);
    }

    [Fact]
    public void Where_Or()
    {
        var result = SproutExpressionVisitor.ConvertWhere<TestUser>(u => u.Age < 18 || u.Age > 60);
        Assert.Equal("age < 18 or age > 60", result);
    }

    // ── Where: string operations ────────────────────────────────

    [Fact]
    public void Where_StringEqual()
    {
        var result = SproutExpressionVisitor.ConvertWhere<TestUser>(u => u.Name == "Alice");
        Assert.Equal("name = 'Alice'", result);
    }

    [Fact]
    public void Where_Contains()
    {
        var result = SproutExpressionVisitor.ConvertWhere<TestUser>(u => u.Name.Contains("li"));
        Assert.Equal("name contains 'li'", result);
    }

    [Fact]
    public void Where_StartsWith()
    {
        var result = SproutExpressionVisitor.ConvertWhere<TestUser>(u => u.Name.StartsWith("Al"));
        Assert.Equal("name starts 'Al'", result);
    }

    [Fact]
    public void Where_EndsWith()
    {
        var result = SproutExpressionVisitor.ConvertWhere<TestUser>(u => u.Name.EndsWith("ce"));
        Assert.Equal("name ends 'ce'", result);
    }

    [Fact]
    public void Where_StringEscape()
    {
        var result = SproutExpressionVisitor.ConvertWhere<TestUser>(u => u.Name == "O'Brien");
        Assert.Equal("name = 'O\\'Brien'", result);
    }

    // ── Where: null checks ──────────────────────────────────────

    [Fact]
    public void Where_NullCheck()
    {
        var result = SproutExpressionVisitor.ConvertWhere<TestUser>(u => u.Name == null);
        Assert.Equal("name is null", result);
    }

    [Fact]
    public void Where_NotNull()
    {
        var result = SproutExpressionVisitor.ConvertWhere<TestUser>(u => u.Name != null);
        Assert.Equal("name is not null", result);
    }

    // ── Where: captured variable ────────────────────────────────

    [Fact]
    public void Where_CapturedVariable()
    {
        byte minAge = 25;
        var result = SproutExpressionVisitor.ConvertWhere<TestUser>(u => u.Age > minAge);
        Assert.Equal("age > 25", result);
    }

    // ── Where: _id mapping ──────────────────────────────────────

    [Fact]
    public void Where_IdMapping()
    {
        var result = SproutExpressionVisitor.ConvertWhere<TestUser>(u => u.Id == 1);
        Assert.Equal("_id = 1", result);
    }

    // ── Select ──────────────────────────────────────────────────

    [Fact]
    public void Select_Single()
    {
        var result = SproutExpressionVisitor.ConvertSelect<TestUser, object>(u => u.Name);
        Assert.Equal(["name"], result);
    }

    [Fact]
    public void Select_Anonymous()
    {
        var result = SproutExpressionVisitor.ConvertSelect<TestUser, object>(u => new { u.Name, u.Age });
        Assert.Equal(["name", "age"], result);
    }

    // ── OrderBy ─────────────────────────────────────────────────

    [Fact]
    public void OrderBy_Column()
    {
        var result = SproutExpressionVisitor.ConvertOrderBy<TestUser, byte>(u => u.Age);
        Assert.Equal("age", result);
    }

    // ── MemberName ──────────────────────────────────────────────

    [Fact]
    public void MemberName_Property()
    {
        var result = SproutExpressionVisitor.ConvertMemberName<TestUser, object>(u => u.Name);
        Assert.Equal("name", result);
    }
}

using Jint;

namespace SproutDB.Core.Tests;

public class AutocompleteTests
{
    private readonly Engine _engine;

    public AutocompleteTests()
    {
        var jsPath = Path.Combine(
            AppContext.BaseDirectory, "..", "..", "..", "..", "..",
            "src", "SproutDB.Core", "wwwroot", "js", "sproutdb-autocomplete.js");
        var js = File.ReadAllText(Path.GetFullPath(jsPath));
        _engine = new Engine();
        _engine.Execute(js);
    }

    private (string Type, string Prefix, string? Table) GetContext(string text, int? pos = null)
    {
        var p = pos ?? text.Length;
        var result = _engine.Evaluate($"SproutAutocomplete.getContext({ToJsString(text)}, {p})");
        var obj = result.AsObject();
        return (
            obj.Get("type").AsString(),
            obj.Get("prefix").AsString(),
            obj.Get("table").IsNull() ? null : obj.Get("table").AsString()
        );
    }

    private (string[] Items, string Cat) GetSuggestions(string text, int? pos = null, string? schemaJson = null)
    {
        var p = pos ?? text.Length;
        var schema = schemaJson ?? "{ tables: [], columns: {} }";
        var result = _engine.Evaluate(
            $"SproutAutocomplete.getSuggestions(SproutAutocomplete.getContext({ToJsString(text)}, {p}), {schema})");
        var obj = result.AsObject();
        var items = obj.Get("items").AsArray();
        var list = new string[items.Length];
        for (uint i = 0; i < items.Length; i++)
            list[i] = items[i].AsString();
        return (list, obj.Get("cat").AsString());
    }

    private static string ToJsString(string s) =>
        "'" + s.Replace("\\", "\\\\").Replace("'", "\\'").Replace("\n", "\\n") + "'";

    // ── Context: Empty / Command ──

    [Fact]
    public void EmptyInput_ReturnsCommand()
    {
        var ctx = GetContext("");
        Assert.Equal("command", ctx.Type);
        Assert.Equal("", ctx.Prefix);
    }

    [Fact]
    public void PartialCommand_ReturnsCommandWithPrefix()
    {
        var ctx = GetContext("ge");
        Assert.Equal("command", ctx.Type);
        Assert.Equal("ge", ctx.Prefix);
    }

    // ── Context: Table ──

    [Fact]
    public void AfterGet_ReturnsTable()
    {
        var ctx = GetContext("get ");
        Assert.Equal("table", ctx.Type);
    }

    [Fact]
    public void AfterUpsert_ReturnsTable()
    {
        var ctx = GetContext("upsert ");
        Assert.Equal("table", ctx.Type);
    }

    [Fact]
    public void AfterDelete_ReturnsTable()
    {
        var ctx = GetContext("delete ");
        Assert.Equal("table", ctx.Type);
    }

    [Fact]
    public void AfterDescribe_ReturnsTable()
    {
        var ctx = GetContext("describe ");
        Assert.Equal("table", ctx.Type);
    }

    [Fact]
    public void AfterGetWithPartial_ReturnsTableWithPrefix()
    {
        var ctx = GetContext("get us");
        Assert.Equal("table", ctx.Type);
        Assert.Equal("us", ctx.Prefix);
    }

    // ── Context: Clause ──

    [Fact]
    public void AfterGetTable_ReturnsClause()
    {
        var ctx = GetContext("get users ");
        Assert.Equal("clause", ctx.Type);
        Assert.Equal("users", ctx.Table);
    }

    [Fact]
    public void AfterGetTableWithPartialClause_ReturnsClauseWithPrefix()
    {
        var ctx = GetContext("get users wh");
        Assert.Equal("clause", ctx.Type);
        Assert.Equal("wh", ctx.Prefix);
        Assert.Equal("users", ctx.Table);
    }

    // ── Context: Column ──

    [Fact]
    public void AfterWhere_ReturnsColumn()
    {
        var ctx = GetContext("get users where ");
        Assert.Equal("column", ctx.Type);
        Assert.Equal("users", ctx.Table);
    }

    [Fact]
    public void AfterSelect_ReturnsColumn()
    {
        var ctx = GetContext("get users select ");
        Assert.Equal("column", ctx.Type);
        Assert.Equal("users", ctx.Table);
    }

    [Fact]
    public void AfterOrderBy_ReturnsColumn()
    {
        var ctx = GetContext("get users order by ");
        Assert.Equal("column", ctx.Type);
        Assert.Equal("users", ctx.Table);
    }

    [Fact]
    public void TableDotPrefix_ReturnsColumn()
    {
        var ctx = GetContext("get users where users.");
        Assert.Equal("column", ctx.Type);
        Assert.Equal("users", ctx.Table);
        Assert.Equal("", ctx.Prefix);
    }

    [Fact]
    public void TableDotPartialColumn_ReturnsColumnWithPrefix()
    {
        var ctx = GetContext("get users where users.na");
        Assert.Equal("column", ctx.Type);
        Assert.Equal("users", ctx.Table);
        Assert.Equal("na", ctx.Prefix);
    }

    // ── Context: Operator ──

    [Fact]
    public void AfterWhereColumn_ReturnsOperator()
    {
        var ctx = GetContext("get users where name ");
        Assert.Equal("operator", ctx.Type);
    }

    [Fact]
    public void AfterAndColumn_ReturnsOperator()
    {
        var ctx = GetContext("get users where name = 'bob' and age ");
        Assert.Equal("operator", ctx.Type);
    }

    // ── Context: Direction ──

    [Fact]
    public void AfterOrderByColumn_ReturnsDirection()
    {
        var ctx = GetContext("get users order by name ");
        Assert.Equal("direction", ctx.Type);
    }

    // ── Context: Boolean/Value ──

    [Fact]
    public void AfterOperator_ReturnsBoolean()
    {
        var ctx = GetContext("get users where active = ");
        Assert.Equal("boolean", ctx.Type);
    }

    // ── Context: Create / Purge sub ──

    [Fact]
    public void AfterCreate_ReturnsCreateSub()
    {
        var ctx = GetContext("create ");
        Assert.Equal("create-sub", ctx.Type);
    }

    [Fact]
    public void AfterPurge_ReturnsPurgeSub()
    {
        var ctx = GetContext("purge ");
        Assert.Equal("purge-sub", ctx.Type);
    }

    [Fact]
    public void AfterPurgeSubcommand_ReturnsTable()
    {
        var ctx = GetContext("purge table ");
        Assert.Equal("table", ctx.Type);
    }

    // ── Context: Type ──

    [Fact]
    public void AfterAddColumnTableCol_ReturnsType()
    {
        var ctx = GetContext("add column users.email ");
        Assert.Equal("type", ctx.Type);
    }

    // ── Context: Upsert body ──

    [Fact]
    public void AfterUpsertTable_ReturnsUpsertBody()
    {
        var ctx = GetContext("upsert users ");
        Assert.Equal("upsert-body", ctx.Type);
        Assert.Equal("users", ctx.Table);
    }

    [Fact]
    public void InsideUpsertBrace_ReturnsColumn()
    {
        var ctx = GetContext("upsert users { ");
        Assert.Equal("column", ctx.Type);
        Assert.Equal("users", ctx.Table);
    }

    [Fact]
    public void InsideUpsertBraceAfterComma_ReturnsColumn()
    {
        var ctx = GetContext("upsert users { name: 'bob', ");
        Assert.Equal("column", ctx.Type);
        Assert.Equal("users", ctx.Table);
    }

    [Fact]
    public void InsideUpsertBraceAfterColon_ReturnsNone()
    {
        var ctx = GetContext("upsert users { name: ");
        Assert.Equal("none", ctx.Type);
    }

    [Fact]
    public void InsideUpsertBracket_ReturnsNone()
    {
        // Inside [ but not inside { — no column suggestions
        var ctx = GetContext("upsert users [ ");
        Assert.Equal("none", ctx.Type);
    }

    [Fact]
    public void InsideUpsertBracketBrace_ReturnsColumn()
    {
        var ctx = GetContext("upsert users [{ ");
        Assert.Equal("column", ctx.Type);
        Assert.Equal("users", ctx.Table);
    }

    [Fact]
    public void InsideUpsertBracketBraceAfterComma_ReturnsColumn()
    {
        var ctx = GetContext("upsert users [{ name: 'bob', ");
        Assert.Equal("column", ctx.Type);
        Assert.Equal("users", ctx.Table);
    }

    // ── Context: Upsert after body ──

    [Fact]
    public void AfterUpsertClosedBrace_ReturnsUpsertAfter()
    {
        var ctx = GetContext("upsert users { name: 'bob' } ");
        Assert.Equal("upsert-after", ctx.Type);
        Assert.Equal("users", ctx.Table);
    }

    [Fact]
    public void AfterUpsertClosedBracket_ReturnsUpsertAfter()
    {
        var ctx = GetContext("upsert users [{ name: 'bob' }] ");
        Assert.Equal("upsert-after", ctx.Type);
        Assert.Equal("users", ctx.Table);
    }

    [Fact]
    public void AfterUpsertBodyOn_ReturnsColumn()
    {
        var ctx = GetContext("upsert users { name: 'bob' } on ");
        Assert.Equal("column", ctx.Type);
        Assert.Equal("users", ctx.Table);
    }

    // ── Suggestions: filtering ──

    [Fact]
    public void CommandSuggestions_FilterByPrefix()
    {
        var (items, cat) = GetSuggestions("g");
        Assert.Equal("cmd", cat);
        Assert.Contains("get", items);
        Assert.DoesNotContain("upsert", items);
    }

    [Fact]
    public void TableSuggestions_UseSchema()
    {
        var (items, cat) = GetSuggestions("get ",
            schemaJson: "{ tables: ['users', 'orders'], columns: {} }");
        Assert.Equal("tbl", cat);
        Assert.Contains("users", items);
        Assert.Contains("orders", items);
    }

    [Fact]
    public void ColumnSuggestions_CaseInsensitiveLookup()
    {
        var (items, cat) = GetSuggestions("get Users where ",
            schemaJson: "{ tables: ['Users'], columns: { 'Users': ['name', 'age', 'email'] } }");
        Assert.Equal("col", cat);
        Assert.Contains("name", items);
        Assert.Contains("age", items);
        Assert.Contains("email", items);
    }

    [Fact]
    public void ClauseSuggestions_FilterByPrefix()
    {
        var (items, cat) = GetSuggestions("get users wh");
        Assert.Equal("kw", cat);
        Assert.Contains("where", items);
        Assert.Single(items);
    }

    [Fact]
    public void UpsertBodySuggestions_ReturnBraceAndBracket()
    {
        var (items, cat) = GetSuggestions("upsert users ");
        Assert.Equal("kw", cat);
        Assert.Contains("{ }", items);
        Assert.Contains("[ ]", items);
    }

    [Fact]
    public void UpsertAfterSuggestions_ReturnOn()
    {
        var (items, cat) = GetSuggestions("upsert users { name: 'bob' } ");
        Assert.Equal("kw", cat);
        Assert.Contains("on", items);
        Assert.Single(items);
    }

    [Fact]
    public void AfterFrom_ReturnsTable()
    {
        var ctx = GetContext("get users select name from ");
        Assert.Equal("table", ctx.Type);
    }
}

using SproutDB.Core.Parsing;

namespace SproutDB.Core.Tests.Parsing;

public class LiteralSelectParserTests
{
    private static GetQuery ParseGet(string query)
    {
        var result = QueryParser.Parse(query);
        Assert.True(result.Success, $"expected parse success, got: {result.Errors?[0].Message}");
        return Assert.IsType<GetQuery>(result.Query);
    }

    // ── Literal types ─────────────────────────────────────────

    [Fact]
    public void Literal_String_WithAlias()
    {
        var q = ParseGet("get routes select host, 'auto' as backend_protocol");

        Assert.NotNull(q.LiteralSelect);
        var lit = Assert.Single(q.LiteralSelect);
        Assert.Equal("auto", lit.Value);
        Assert.Equal("backend_protocol", lit.Alias);
        Assert.NotNull(q.Select);
        Assert.Single(q.Select);
    }

    [Fact]
    public void Literal_Integer_WithAlias()
    {
        var q = ParseGet("get routes select host, 1 as version");

        Assert.NotNull(q.LiteralSelect);
        var lit = Assert.Single(q.LiteralSelect);
        Assert.Equal(1L, lit.Value);
        Assert.Equal("version", lit.Alias);
    }

    [Fact]
    public void Literal_Float_WithAlias()
    {
        var q = ParseGet("get routes select host, 2.5 as factor");

        Assert.NotNull(q.LiteralSelect);
        var lit = Assert.Single(q.LiteralSelect);
        Assert.Equal(2.5, lit.Value);
    }

    [Fact]
    public void Literal_NegativeInteger()
    {
        var q = ParseGet("get routes select host, -1 as offset");

        Assert.NotNull(q.LiteralSelect);
        var lit = Assert.Single(q.LiteralSelect);
        Assert.Equal(-1L, lit.Value);
    }

    [Fact]
    public void Literal_NegativeFloat()
    {
        var q = ParseGet("get routes select host, -2.5 as delta");

        Assert.NotNull(q.LiteralSelect);
        var lit = Assert.Single(q.LiteralSelect);
        Assert.Equal(-2.5, lit.Value);
    }

    [Fact]
    public void Literal_True()
    {
        var q = ParseGet("get routes select host, true as preserve_host");

        Assert.NotNull(q.LiteralSelect);
        var lit = Assert.Single(q.LiteralSelect);
        Assert.Equal(true, lit.Value);
        Assert.Equal("preserve_host", lit.Alias);
    }

    [Fact]
    public void Literal_False()
    {
        var q = ParseGet("get routes select host, false as disabled");

        Assert.NotNull(q.LiteralSelect);
        Assert.Equal(false, Assert.Single(q.LiteralSelect).Value);
    }

    [Fact]
    public void Literal_Null()
    {
        var q = ParseGet("get routes select host, null as cert_path");

        Assert.NotNull(q.LiteralSelect);
        var lit = Assert.Single(q.LiteralSelect);
        Assert.Null(lit.Value);
        Assert.Equal("cert_path", lit.Alias);
    }

    [Fact]
    public void Literal_BoolKeyword_CaseInsensitive()
    {
        var q = ParseGet("get routes select host, TRUE as flag");

        Assert.NotNull(q.LiteralSelect);
        Assert.Equal(true, Assert.Single(q.LiteralSelect).Value);
    }

    [Fact]
    public void Literal_StringWithEscapedQuote()
    {
        var q = ParseGet(@"get routes select host, 'it\'s' as note");

        Assert.NotNull(q.LiteralSelect);
        Assert.Equal("it's", Assert.Single(q.LiteralSelect).Value);
    }

    // ── Alias is mandatory ────────────────────────────────────

    [Fact]
    public void Literal_String_WithoutAlias_Error()
    {
        var result = QueryParser.Parse("get routes select host, 'auto'");

        Assert.False(result.Success);
        Assert.Equal("SYNTAX_ERROR", result.Errors![0].Code);
        Assert.Contains("literal in select requires an alias", result.Errors[0].Message);
    }

    [Fact]
    public void Literal_Integer_WithoutAlias_Error()
    {
        var result = QueryParser.Parse("get routes select host, 1");

        Assert.False(result.Success);
        Assert.Contains("literal in select requires an alias", result.Errors![0].Message);
    }

    [Fact]
    public void Literal_AliasNotIdentifier_Error()
    {
        var result = QueryParser.Parse("get routes select host, 1 as 'x'");

        Assert.False(result.Success);
        Assert.Contains("expected alias name after 'as'", result.Errors![0].Message);
    }

    // ── true/false/null only count as literals before 'as' ────

    [Fact]
    public void Bool_WithoutAs_StaysColumnReference()
    {
        // Regression: 'true' is an Identifier token. Without a following 'as' it must keep
        // meaning "the column named true", exactly as before this feature existed.
        var q = ParseGet("get routes select host, true");

        Assert.Null(q.LiteralSelect);
        Assert.NotNull(q.Select);
        Assert.Equal(2, q.Select.Count);
        Assert.Equal("true", q.Select[1].Name);
    }

    [Fact]
    public void Null_WithoutAs_StaysColumnReference()
    {
        var q = ParseGet("get routes select null");

        Assert.Null(q.LiteralSelect);
        Assert.NotNull(q.Select);
        Assert.Equal("null", Assert.Single(q.Select).Name);
    }

    // ── Arithmetic stays arithmetic ───────────────────────────

    [Fact]
    public void ComputedColumn_MinusLiteral_NotParsedAsLiteral()
    {
        // Regression: without a comma this is "price - 1", not a literal -1.
        var q = ParseGet("get orders select price -1 as x");

        Assert.Null(q.LiteralSelect);
        Assert.NotNull(q.ComputedSelect);
        var comp = Assert.Single(q.ComputedSelect);
        Assert.Equal("price", comp.LeftColumn);
        Assert.Equal(ArithmeticOp.Subtract, comp.Operator);
        Assert.Equal(1.0, comp.RightLiteral);
    }

    [Fact]
    public void ComputedColumn_WithLiteralOperand_StillWorks()
    {
        var q = ParseGet("get orders select price * 1.2 as gross");

        Assert.Null(q.LiteralSelect);
        Assert.NotNull(q.ComputedSelect);
        Assert.Equal(1.2, Assert.Single(q.ComputedSelect).RightLiteral);
    }

    // ── Composition ───────────────────────────────────────────

    [Fact]
    public void Literal_Only_NoRealColumn_IsValid()
    {
        var q = ParseGet("get routes select 1 as x");

        Assert.NotNull(q.LiteralSelect);
        Assert.Equal(1L, Assert.Single(q.LiteralSelect).Value);
        Assert.NotNull(q.Select);
        Assert.Empty(q.Select);
    }

    [Fact]
    public void Literal_MultipleLiterals()
    {
        var q = ParseGet("get routes select host, true as a, 'x' as b, 3 as c");

        Assert.NotNull(q.LiteralSelect);
        Assert.Equal(3, q.LiteralSelect.Count);
        Assert.Equal("a", q.LiteralSelect[0].Alias);
        Assert.Equal("b", q.LiteralSelect[1].Alias);
        Assert.Equal("c", q.LiteralSelect[2].Alias);
    }

    [Fact]
    public void Literal_MixedWithColumnsAndComputed()
    {
        var q = ParseGet("get orders select name, 1 as a, price * 2 as b, 'x' as c");

        Assert.NotNull(q.Select);
        Assert.Single(q.Select);
        Assert.NotNull(q.ComputedSelect);
        Assert.Single(q.ComputedSelect);
        Assert.NotNull(q.LiteralSelect);
        Assert.Equal(2, q.LiteralSelect.Count);
    }

    [Fact]
    public void Literal_LeadingPosition()
    {
        var q = ParseGet("get routes select 1 as a, host");

        Assert.NotNull(q.LiteralSelect);
        Assert.NotNull(q.Select);
        // Position must place the literal before the column for the projection merge.
        Assert.True(q.LiteralSelect[0].Position < q.Select[0].Position);
    }

    [Fact]
    public void Literal_WithTrailingClauses()
    {
        var q = ParseGet("get routes select host, true as flag where host = 'a' order by host limit 5");

        Assert.NotNull(q.LiteralSelect);
        Assert.Single(q.LiteralSelect);
        Assert.NotNull(q.Where);
        Assert.NotNull(q.OrderBy);
        Assert.Equal(5, q.Limit);
    }

    // ── -select rejects literals ──────────────────────────────

    [Fact]
    public void Literal_InExcludeSelect_Error()
    {
        var result = QueryParser.Parse("get routes -select host, 1 as x");

        Assert.False(result.Success);
        Assert.Equal("SYNTAX_ERROR", result.Errors![0].Code);
        Assert.Contains("literals are not allowed in '-select'", result.Errors[0].Message);
    }

    [Fact]
    public void Literal_InPostFollowExcludeSelect_Error()
    {
        var result = QueryParser.Parse(
            "get routes follow routes._id -> backends.route_id as b -select 1 as x");

        Assert.False(result.Success);
        Assert.Contains("literals are not allowed in '-select'", result.Errors![0].Message);
    }

    // ── Post-follow ───────────────────────────────────────────

    [Fact]
    public void Literal_InPostFollowSelect()
    {
        var q = ParseGet(
            "get routes follow routes._id -> backends.route_id as b select host, b.name, true as ha");

        Assert.NotNull(q.PostFollowLiteralSelect);
        var lit = Assert.Single(q.PostFollowLiteralSelect);
        Assert.Equal(true, lit.Value);
        Assert.Equal("ha", lit.Alias);
        Assert.Null(q.LiteralSelect);
    }

    [Fact]
    public void Literal_InBaseSelectAndPostFollowSelect()
    {
        var q = ParseGet(
            "get routes select host, 1 as v follow routes._id -> backends.route_id as b select host, 2 as w");

        Assert.NotNull(q.LiteralSelect);
        Assert.Equal("v", Assert.Single(q.LiteralSelect).Alias);
        Assert.NotNull(q.PostFollowLiteralSelect);
        Assert.Equal("w", Assert.Single(q.PostFollowLiteralSelect).Alias);
    }

    // ── Post-follow detection (no dot in the list) ────────────

    [Fact]
    public void PostFollow_LiteralWithoutDotColumn_IsPostFollowSelect()
    {
        // The literal is what tells the parser this can't be a follow-level select.
        var q = ParseGet("get routes follow routes._id -> backends.route_id as b select host, true as ha");

        Assert.NotNull(q.PostFollowLiteralSelect);
        Assert.Equal("ha", Assert.Single(q.PostFollowLiteralSelect).Alias);
        Assert.NotNull(q.Follow);
        Assert.Null(Assert.Single(q.Follow).Select);
    }

    [Fact]
    public void PostFollow_ComputedWithoutDotColumn_IsPostFollowSelect()
    {
        // Regression for a pre-existing bug: this used to be routed to the follow-level
        // parser, which stopped at '*' and left the rest to fail as "expected end of query".
        var q = ParseGet("get orders follow orders._id -> items.order_id as i select name, price * 2 as x");

        Assert.NotNull(q.PostFollowComputedSelect);
        var comp = Assert.Single(q.PostFollowComputedSelect);
        Assert.Equal("price", comp.LeftColumn);
        Assert.Equal("x", comp.Alias);
    }

    [Fact]
    public void PostFollow_ComputedOnly_IsPostFollowSelect()
    {
        var q = ParseGet("get orders follow orders._id -> items.order_id as i select price * 2 as x");

        Assert.NotNull(q.PostFollowComputedSelect);
        Assert.Single(q.PostFollowComputedSelect);
    }

    [Fact]
    public void PostFollow_PlainColumnsOnly_StaysFollowLevelSelect()
    {
        // No dot, no literal, no arithmetic → unchanged: this selects the target's columns.
        var q = ParseGet("get routes follow routes._id -> backends.route_id as b select name");

        Assert.Null(q.PostFollowSelect);
        Assert.NotNull(q.Follow);
        var follow = Assert.Single(q.Follow);
        Assert.NotNull(follow.Select);
        Assert.Equal("name", Assert.Single(follow.Select).Name);
    }

    [Fact]
    public void PostFollow_PlainColumnsWithAlias_StaysFollowLevelSelect()
    {
        var q = ParseGet("get routes follow routes._id -> backends.route_id as b select name as n");

        Assert.Null(q.PostFollowSelect);
        Assert.NotNull(q.Follow);
        var follow = Assert.Single(q.Follow);
        Assert.NotNull(follow.Select);
        Assert.Equal("n", Assert.Single(follow.Select).Alias);
    }

    [Fact]
    public void PostFollow_FollowLevelSelectWithWhere_Unchanged()
    {
        // The scan must stop at 'where' and not mistake anything after it for a signal.
        var q = ParseGet(
            "get routes follow routes._id -> backends.route_id as b select name where port > 80");

        Assert.Null(q.PostFollowSelect);
        Assert.NotNull(q.Follow);
        var follow = Assert.Single(q.Follow);
        Assert.NotNull(follow.Select);
        Assert.NotNull(follow.Where);
    }

    // ── Follow-level select terminators (pre-existing bug, fixed here) ──

    [Fact]
    public void FollowLevelSelect_StopsAtLimit()
    {
        // Regression: 'limit' used to be read as a column name, and the 5 then killed the
        // query with "unexpected token, expected end of query".
        var q = ParseGet("get users follow users._id -> orders.user_id as o select name limit 5");

        Assert.NotNull(q.Follow);
        var follow = Assert.Single(q.Follow);
        Assert.NotNull(follow.Select);
        Assert.Equal("name", Assert.Single(follow.Select).Name);
        Assert.Equal(5, q.Limit);
    }

    [Fact]
    public void FollowLevelSelect_StopsAtCount()
    {
        // Regression: this used to "succeed" with a bogus column named 'count'.
        var q = ParseGet("get users follow users._id -> orders.user_id as o select name count");

        Assert.NotNull(q.Follow);
        var follow = Assert.Single(q.Follow);
        Assert.NotNull(follow.Select);
        Assert.Equal("name", Assert.Single(follow.Select).Name);
        Assert.True(q.IsCount);
    }

    [Fact]
    public void FollowLevelSelect_StopsAtOrderBy()
    {
        // Regression: used to yield columns name|order|by|name.
        var q = ParseGet("get users follow users._id -> orders.user_id as o select name order by name");

        Assert.NotNull(q.Follow);
        var follow = Assert.Single(q.Follow);
        Assert.NotNull(follow.Select);
        Assert.Equal("name", Assert.Single(follow.Select).Name);
        Assert.NotNull(q.OrderBy);
        Assert.Equal("name", Assert.Single(q.OrderBy).Name);
    }

    [Fact]
    public void FollowLevelSelect_StopsAtPage()
    {
        var q = ParseGet("get users follow users._id -> orders.user_id as o select name page 2 size 10");

        Assert.NotNull(q.Follow);
        var follow = Assert.Single(q.Follow);
        Assert.NotNull(follow.Select);
        Assert.Equal("name", Assert.Single(follow.Select).Name);
        Assert.Equal(2, q.Page);
        Assert.Equal(10, q.Size);
    }

    [Fact]
    public void FollowLevelSelect_StopsAtWhere_Unchanged()
    {
        var q = ParseGet("get users follow users._id -> orders.user_id as o select product where total > 50");

        Assert.NotNull(q.Follow);
        var follow = Assert.Single(q.Follow);
        Assert.NotNull(follow.Select);
        Assert.Equal("product", Assert.Single(follow.Select).Name);
        Assert.NotNull(follow.Where);
    }

    // ── Select-list error paths (touched by the 3-tuple refactor) ──

    [Fact]
    public void Select_DotNotation_MissingSubColumn_Error()
    {
        var result = QueryParser.Parse("get users select o.1");

        Assert.False(result.Success);
        Assert.Equal("SYNTAX_ERROR", result.Errors![0].Code);
        Assert.Contains("expected column name", result.Errors[0].Message);
    }

    [Fact]
    public void Select_DotNotation_AliasNotIdentifier_Error()
    {
        var result = QueryParser.Parse("get users select o.name as 'x'");

        Assert.False(result.Success);
        Assert.Contains("expected alias name after 'as'", result.Errors![0].Message);
    }

    [Fact]
    public void Select_DotNotation_BrokenArithmetic_Error()
    {
        // 'as' is an Identifier token, so it gets consumed as the right operand and the
        // complaint lands on the then-missing alias rather than on the operator.
        var result = QueryParser.Parse("get users select o.price * as x");

        Assert.False(result.Success);
        Assert.Equal("SYNTAX_ERROR", result.Errors![0].Code);
        Assert.Contains("computed field requires 'as <alias>'", result.Errors[0].Message);
    }

    [Fact]
    public void Select_DotNotation_OperatorWithoutOperand_Error()
    {
        var result = QueryParser.Parse("get users select o.price * , name");

        Assert.False(result.Success);
        Assert.Contains("expected column name or number after operator", result.Errors![0].Message);
    }

    [Fact]
    public void Select_MinusFollowedByIdentifier_IsNotALiteral()
    {
        // Only '-' directly before a number starts a literal. '-x' is neither a literal
        // nor a column, so it must fall through to the plain column-name error.
        var result = QueryParser.Parse("get users select name, -x as y");

        Assert.False(result.Success);
        Assert.Equal("SYNTAX_ERROR", result.Errors![0].Code);
        Assert.Contains("expected column name", result.Errors[0].Message);
    }

    [Fact]
    public void Select_ImmediatelyFollowedByStopKeyword_Error()
    {
        // The list breaks on the very first token, so nothing is collected — a different
        // path from "select" at end of input.
        var result = QueryParser.Parse("get users select where name = 'x'");

        Assert.False(result.Success);
        Assert.Equal("SYNTAX_ERROR", result.Errors![0].Code);
        Assert.Contains("expected column name", result.Errors[0].Message);
    }

    // ── Duplicate output names ────────────────────────────────

    [Fact]
    public void Duplicate_BareColumnRepeated_IsAllowed()
    {
        // Harmless repetition, legal before this feature and still legal.
        var q = ParseGet("get routes select host, host");

        Assert.NotNull(q.Select);
        Assert.Equal(2, q.Select.Count);
    }

    [Fact]
    public void Duplicate_LiteralOverColumn_Error()
    {
        var result = QueryParser.Parse("get routes select host, 1 as host");

        Assert.False(result.Success);
        Assert.Equal("SYNTAX_ERROR", result.Errors![0].Code);
        Assert.Contains("duplicate output name 'host'", result.Errors[0].Message);
    }

    [Fact]
    public void Duplicate_TwoAliasedColumns_Error()
    {
        var result = QueryParser.Parse("get routes select host as x, port as x");

        Assert.False(result.Success);
        Assert.Contains("duplicate output name 'x'", result.Errors![0].Message);
    }

    [Fact]
    public void Duplicate_TwoLiterals_Error()
    {
        var result = QueryParser.Parse("get routes select 1 as x, 2 as x");

        Assert.False(result.Success);
        Assert.Contains("duplicate output name 'x'", result.Errors![0].Message);
    }

    [Fact]
    public void Duplicate_TwoComputedColumns_Error_Breaking()
    {
        // BREAKING: this was silently allowed before (last writer won). No existing test
        // covered it — that gap is why the suite stayed green when the check went in.
        var result = QueryParser.Parse("get orders select price * 2 as x, quantity * 3 as x");

        Assert.False(result.Success);
        Assert.Contains("duplicate output name 'x'", result.Errors![0].Message);
    }

    [Fact]
    public void Duplicate_ComputedOverLiteral_Error()
    {
        var result = QueryParser.Parse("get orders select price * 2 as x, 1 as x");

        Assert.False(result.Success);
        Assert.Contains("duplicate output name 'x'", result.Errors![0].Message);
    }

    [Fact]
    public void Duplicate_AliasMatchingOtherBareColumn_Error()
    {
        var result = QueryParser.Parse("get routes select host, port as host");

        Assert.False(result.Success);
        Assert.Contains("duplicate output name 'host'", result.Errors![0].Message);
    }

    [Fact]
    public void Duplicate_DistinctAliases_Allowed()
    {
        var q = ParseGet("get routes select host as a, port as b, 1 as c");

        Assert.NotNull(q.Select);
        Assert.Equal(2, q.Select.Count);
        Assert.NotNull(q.LiteralSelect);
        Assert.Single(q.LiteralSelect);
    }

    [Fact]
    public void Duplicate_InExcludeSelect_NotChecked()
    {
        // Exclude mode removes by name; a repeat is a no-op, not a conflict.
        var q = ParseGet("get routes -select host, host");

        Assert.True(q.ExcludeSelect);
        Assert.NotNull(q.Select);
        Assert.Equal(2, q.Select.Count);
    }

    [Fact]
    public void Duplicate_CaseInsensitive_Error()
    {
        var result = QueryParser.Parse("get routes select host as x, port as X");

        Assert.False(result.Success);
        Assert.Contains("duplicate output name", result.Errors![0].Message);
    }

    [Fact]
    public void Duplicate_BaseAndPostFollow_SeparateNamespaces()
    {
        // 'x' in the base select and 'x' in the post-follow select don't collide —
        // the post-follow list is a fresh projection over the joined rows.
        var q = ParseGet(
            "get routes select host, 1 as x follow routes._id -> backends.route_id as b select host, 2 as x");

        Assert.NotNull(q.LiteralSelect);
        Assert.Equal("x", Assert.Single(q.LiteralSelect).Alias);
        Assert.NotNull(q.PostFollowLiteralSelect);
        Assert.Equal("x", Assert.Single(q.PostFollowLiteralSelect).Alias);
    }

    [Fact]
    public void Duplicate_WithinPostFollowSelect_Error()
    {
        var result = QueryParser.Parse(
            "get routes follow routes._id -> backends.route_id as b select 1 as x, 2 as x");

        Assert.False(result.Success);
        Assert.Contains("duplicate output name 'x'", result.Errors![0].Message);
    }
}

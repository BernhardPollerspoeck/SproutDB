using SproutDB.Core.Parsing;

namespace SproutDB.Core.Tests.Auth;

public class AuthParserTests
{
    // ── create apikey ──────────────────────────────────────

    [Fact]
    public void CreateApiKey_ParsesName()
    {
        var result = QueryParser.Parse("create apikey 'backend-service'");

        Assert.True(result.Success);
        var q = Assert.IsType<CreateApiKeyQuery>(result.Query);
        Assert.Equal("backend-service", q.Name);
    }

    [Fact]
    public void CreateApiKey_CaseInsensitive()
    {
        var result = QueryParser.Parse("CREATE APIKEY 'my-key'");

        Assert.True(result.Success);
        var q = Assert.IsType<CreateApiKeyQuery>(result.Query);
        Assert.Equal("my-key", q.Name);
    }

    [Fact]
    public void CreateApiKey_MissingName_Error()
    {
        var result = QueryParser.Parse("create apikey");

        Assert.False(result.Success);
    }

    [Fact]
    public void CreateApiKey_UnquotedName_Error()
    {
        var result = QueryParser.Parse("create apikey mykey");

        Assert.False(result.Success);
        Assert.Contains("string literal", result.Errors![0].Message);
    }

    [Fact]
    public void CreateApiKey_ExtraTokens_Error()
    {
        var result = QueryParser.Parse("create apikey 'mykey' extra");

        Assert.False(result.Success);
        Assert.Contains("expected end of query", result.Errors![0].Message);
    }

    // ── purge apikey ──────────────────────────────────────

    [Fact]
    public void PurgeApiKey_ParsesName()
    {
        var result = QueryParser.Parse("purge apikey 'backend-service'");

        Assert.True(result.Success);
        var q = Assert.IsType<PurgeApiKeyQuery>(result.Query);
        Assert.Equal("backend-service", q.Name);
    }

    [Fact]
    public void PurgeApiKey_MissingName_Error()
    {
        var result = QueryParser.Parse("purge apikey");

        Assert.False(result.Success);
    }

    // ── rotate apikey ─────────────────────────────────────

    [Fact]
    public void RotateApiKey_ParsesName()
    {
        var result = QueryParser.Parse("rotate apikey 'backend-service'");

        Assert.True(result.Success);
        var q = Assert.IsType<RotateApiKeyQuery>(result.Query);
        Assert.Equal("backend-service", q.Name);
    }

    [Fact]
    public void RotateApiKey_MissingApikey_Error()
    {
        var result = QueryParser.Parse("rotate");

        Assert.False(result.Success);
        Assert.Contains("expected 'apikey'", result.Errors![0].Message);
    }

    [Fact]
    public void RotateApiKey_WrongTarget_Error()
    {
        var result = QueryParser.Parse("rotate something");

        Assert.False(result.Success);
        Assert.Contains("expected 'apikey'", result.Errors![0].Message);
    }

    // ── grant ──────────────────────────────────────────────

    [Fact]
    public void Grant_ParsesRoleDatabaseAndKey()
    {
        var result = QueryParser.Parse("grant writer on shop to 'backend-service'");

        Assert.True(result.Success);
        var q = Assert.IsType<GrantQuery>(result.Query);
        Assert.Equal("writer", q.Role);
        Assert.Equal("shop", q.Database);
        Assert.Equal("backend-service", q.KeyName);
    }

    [Fact]
    public void Grant_AdminRole()
    {
        var result = QueryParser.Parse("grant admin on metrics to 'admin-key'");

        Assert.True(result.Success);
        var q = Assert.IsType<GrantQuery>(result.Query);
        Assert.Equal("admin", q.Role);
    }

    [Fact]
    public void Grant_ReaderRole()
    {
        var result = QueryParser.Parse("grant reader on logs to 'readonly-key'");

        Assert.True(result.Success);
        var q = Assert.IsType<GrantQuery>(result.Query);
        Assert.Equal("reader", q.Role);
    }

    [Fact]
    public void Grant_CaseInsensitive()
    {
        var result = QueryParser.Parse("GRANT Writer ON Shop TO 'key'");

        Assert.True(result.Success);
        var q = Assert.IsType<GrantQuery>(result.Query);
        Assert.Equal("writer", q.Role);
        Assert.Equal("shop", q.Database);
    }

    [Fact]
    public void Grant_InvalidRole_Error()
    {
        var result = QueryParser.Parse("grant superuser on shop to 'key'");

        Assert.False(result.Success);
        Assert.Contains("expected role", result.Errors![0].Message);
    }

    [Fact]
    public void Grant_MissingOn_Error()
    {
        var result = QueryParser.Parse("grant writer shop to 'key'");

        Assert.False(result.Success);
        Assert.Contains("expected 'on'", result.Errors![0].Message);
    }

    [Fact]
    public void Grant_MissingTo_Error()
    {
        var result = QueryParser.Parse("grant writer on shop 'key'");

        Assert.False(result.Success);
        Assert.Contains("expected 'to'", result.Errors![0].Message);
    }

    // ── revoke ─────────────────────────────────────────────

    [Fact]
    public void Revoke_ParsesDatabaseAndKey()
    {
        var result = QueryParser.Parse("revoke shop from 'backend-service'");

        Assert.True(result.Success);
        var q = Assert.IsType<RevokeQuery>(result.Query);
        Assert.Equal("shop", q.Database);
        Assert.Equal("backend-service", q.KeyName);
    }

    [Fact]
    public void Revoke_MissingFrom_Error()
    {
        var result = QueryParser.Parse("revoke shop 'key'");

        Assert.False(result.Success);
        Assert.Contains("expected 'from'", result.Errors![0].Message);
    }

    // ── restrict ───────────────────────────────────────────

    [Fact]
    public void Restrict_TableToReader()
    {
        var result = QueryParser.Parse("restrict orders to reader for 'backend-service' on shop");

        Assert.True(result.Success);
        var q = Assert.IsType<RestrictQuery>(result.Query);
        Assert.Equal("orders", q.Table);
        Assert.Equal("reader", q.Role);
        Assert.Equal("backend-service", q.KeyName);
        Assert.Equal("shop", q.Database);
    }

    [Fact]
    public void Restrict_WildcardToNone()
    {
        var result = QueryParser.Parse("restrict * to none for 'backend-service' on shop");

        Assert.True(result.Success);
        var q = Assert.IsType<RestrictQuery>(result.Query);
        Assert.Equal("*", q.Table);
        Assert.Equal("none", q.Role);
    }

    [Fact]
    public void Restrict_InvalidRole_Error()
    {
        var result = QueryParser.Parse("restrict orders to writer for 'key' on shop");

        Assert.False(result.Success);
        Assert.Contains("expected role (reader, none)", result.Errors![0].Message);
    }

    [Fact]
    public void Restrict_MissingFor_Error()
    {
        var result = QueryParser.Parse("restrict orders to reader 'key' on shop");

        Assert.False(result.Success);
        Assert.Contains("expected 'for'", result.Errors![0].Message);
    }

    [Fact]
    public void Restrict_MissingOn_Error()
    {
        var result = QueryParser.Parse("restrict orders to reader for 'key' shop");

        Assert.False(result.Success);
        Assert.Contains("expected 'on'", result.Errors![0].Message);
    }

    // ── unrestrict ─────────────────────────────────────────

    [Fact]
    public void Unrestrict_ParsesAll()
    {
        var result = QueryParser.Parse("unrestrict orders for 'backend-service' on shop");

        Assert.True(result.Success);
        var q = Assert.IsType<UnrestrictQuery>(result.Query);
        Assert.Equal("orders", q.Table);
        Assert.Equal("backend-service", q.KeyName);
        Assert.Equal("shop", q.Database);
    }

    [Fact]
    public void Unrestrict_MissingFor_Error()
    {
        var result = QueryParser.Parse("unrestrict orders 'key' on shop");

        Assert.False(result.Success);
        Assert.Contains("expected 'for'", result.Errors![0].Message);
    }

    // ── SproutOperation values ─────────────────────────────

    [Fact]
    public void AuthQueries_HaveCorrectOperations()
    {
        var tests = new (string Query, SproutOperation Expected)[]
        {
            ("create apikey 'k'", SproutOperation.CreateApiKey),
            ("purge apikey 'k'", SproutOperation.PurgeApiKey),
            ("rotate apikey 'k'", SproutOperation.RotateApiKey),
            ("grant reader on db to 'k'", SproutOperation.Grant),
            ("revoke db from 'k'", SproutOperation.Revoke),
            ("restrict t to reader for 'k' on db", SproutOperation.Restrict),
            ("unrestrict t for 'k' on db", SproutOperation.Unrestrict),
        };

        foreach (var (query, expected) in tests)
        {
            var result = QueryParser.Parse(query);
            Assert.True(result.Success, $"Failed to parse: {query}");
            Assert.Equal(expected, result.Query!.Operation);
        }
    }
}

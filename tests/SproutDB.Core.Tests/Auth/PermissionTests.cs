namespace SproutDB.Core.Tests.Auth;

public sealed class PermissionTests : IDisposable
{
    private readonly string _dataDir = Path.Combine(Path.GetTempPath(), $"sproutdb-auth-{Guid.NewGuid()}");
    private readonly SproutEngine _engine;
    private readonly string _masterKey = "sdb_ak_testmasterkey1234567890abcdef12";

    public PermissionTests()
    {
        _engine = new SproutEngine(new SproutEngineSettings
        {
            DataDirectory = _dataDir,
            MasterKey = _masterKey,
            FlushInterval = Timeout.InfiniteTimeSpan,
            WalSyncInterval = TimeSpan.Zero,
        });
    }

    public void Dispose()
    {
        _engine.Dispose();
        if (Directory.Exists(_dataDir))
            Directory.Delete(_dataDir, true);
    }

    // ── Key Management ─────────────────────────────────────

    [Fact]
    public void CreateApiKey_ReturnsKeyInResponse()
    {
        var response = _engine.Execute("create apikey 'test-key'", "_system");

        Assert.Null(response.Errors);
        Assert.Equal(SproutOperation.CreateApiKey, response.Operation);
        Assert.Equal(1, response.Affected);
        Assert.NotNull(response.Data);
        Assert.Single(response.Data);

        var row = response.Data[0];
        Assert.Equal("test-key", row["name"]);
        Assert.NotNull(row["api_key"]);

        var apiKey = row["api_key"]!.ToString();
        Assert.StartsWith("sdb_ak_", apiKey);
        Assert.NotNull(row["key_prefix"]);
        Assert.NotNull(row["created_at"]);
    }

    [Fact]
    public void CreateApiKey_DuplicateName_ReturnsError()
    {
        _engine.Execute("create apikey 'dup-key'", "_system");
        var response = _engine.Execute("create apikey 'dup-key'", "_system");

        Assert.NotNull(response.Errors);
        Assert.Contains(response.Errors, e => e.Code == "KEY_EXISTS");
    }

    [Fact]
    public void PurgeApiKey_RemovesKey()
    {
        _engine.Execute("create apikey 'temp-key'", "_system");
        var response = _engine.Execute("purge apikey 'temp-key'", "_system");

        Assert.Null(response.Errors);
        Assert.Equal(SproutOperation.PurgeApiKey, response.Operation);

        // Can re-create the key
        var recreate = _engine.Execute("create apikey 'temp-key'", "_system");
        Assert.Null(recreate.Errors);
    }

    [Fact]
    public void PurgeApiKey_NonExistent_ReturnsError()
    {
        var response = _engine.Execute("purge apikey 'ghost'", "_system");

        Assert.NotNull(response.Errors);
        Assert.Contains(response.Errors, e => e.Code == "KEY_NOT_FOUND");
    }

    [Fact]
    public void RotateApiKey_ReturnsNewKey()
    {
        var create = _engine.Execute("create apikey 'rotate-key'", "_system");
        var originalKey = create.Data![0]["api_key"]!.ToString();

        var response = _engine.Execute("rotate apikey 'rotate-key'", "_system");

        Assert.Null(response.Errors);
        Assert.Equal(SproutOperation.RotateApiKey, response.Operation);
        Assert.NotNull(response.Data);

        var newKey = response.Data[0]["api_key"]!.ToString();
        Assert.StartsWith("sdb_ak_", newKey);
        Assert.NotEqual(originalKey, newKey);
    }

    [Fact]
    public void RotateApiKey_NonExistent_ReturnsError()
    {
        var response = _engine.Execute("rotate apikey 'ghost'", "_system");

        Assert.NotNull(response.Errors);
        Assert.Contains(response.Errors, e => e.Code == "KEY_NOT_FOUND");
    }

    // ── Grant / Revoke ─────────────────────────────────────

    [Fact]
    public void Grant_WriterOnDatabase()
    {
        _engine.Execute("create apikey 'grant-key'", "_system");
        var response = _engine.Execute("grant writer on shop to 'grant-key'", "_system");

        Assert.Null(response.Errors);
        Assert.Equal(SproutOperation.Grant, response.Operation);
    }

    [Fact]
    public void Grant_NonExistentKey_ReturnsError()
    {
        var response = _engine.Execute("grant writer on shop to 'ghost'", "_system");

        Assert.NotNull(response.Errors);
        Assert.Contains(response.Errors, e => e.Code == "KEY_NOT_FOUND");
    }

    [Fact]
    public void Revoke_RemovesAccess()
    {
        _engine.Execute("create apikey 'revoke-key'", "_system");
        _engine.Execute("grant writer on shop to 'revoke-key'", "_system");

        var response = _engine.Execute("revoke shop from 'revoke-key'", "_system");

        Assert.Null(response.Errors);
        Assert.Equal(SproutOperation.Revoke, response.Operation);
    }

    // ── Restrict / Unrestrict ──────────────────────────────

    [Fact]
    public void Restrict_TableToReader()
    {
        _engine.Execute("create apikey 'restrict-key'", "_system");
        _engine.Execute("grant writer on shop to 'restrict-key'", "_system");

        var response = _engine.Execute("restrict orders to reader for 'restrict-key' on shop", "_system");

        Assert.Null(response.Errors);
        Assert.Equal(SproutOperation.Restrict, response.Operation);
    }

    [Fact]
    public void Restrict_WildcardToNone()
    {
        _engine.Execute("create apikey 'wild-key'", "_system");
        _engine.Execute("grant admin on shop to 'wild-key'", "_system");

        var response = _engine.Execute("restrict * to none for 'wild-key' on shop", "_system");

        Assert.Null(response.Errors);
    }

    [Fact]
    public void Unrestrict_RemovesRestriction()
    {
        _engine.Execute("create apikey 'unrest-key'", "_system");
        _engine.Execute("grant writer on shop to 'unrest-key'", "_system");
        _engine.Execute("restrict orders to reader for 'unrest-key' on shop", "_system");

        var response = _engine.Execute("unrestrict orders for 'unrest-key' on shop", "_system");

        Assert.Null(response.Errors);
        Assert.Equal(SproutOperation.Unrestrict, response.Operation);
    }

    // ── Permission checks ──────────────────────────────────

    [Fact]
    public void AuthService_ValidatesCreatedKey()
    {
        var create = _engine.Execute("create apikey 'val-key'", "_system");
        var apiKey = create.Data![0]["api_key"]!.ToString()!;

        var entry = _engine.AuthService!.ValidateKey(apiKey);
        Assert.NotNull(entry);
        Assert.Equal("val-key", entry.Name);
    }

    [Fact]
    public void AuthService_MasterKey_IsRecognized()
    {
        Assert.True(_engine.AuthService!.IsMasterKey(_masterKey));
    }

    [Fact]
    public void AuthService_InvalidKey_ReturnsNull()
    {
        var entry = _engine.AuthService!.ValidateKey("sdb_ak_invalidkey12345678901234567");
        Assert.Null(entry);
    }

    [Fact]
    public void AuthService_RotatedKey_OldKeyInvalid()
    {
        var create = _engine.Execute("create apikey 'rot-check'", "_system");
        var oldKey = create.Data![0]["api_key"]!.ToString()!;

        var rotate = _engine.Execute("rotate apikey 'rot-check'", "_system");
        var newKey = rotate.Data![0]["api_key"]!.ToString()!;

        Assert.Null(_engine.AuthService!.ValidateKey(oldKey));
        Assert.NotNull(_engine.AuthService.ValidateKey(newKey));
    }

    [Fact]
    public void AuthService_PurgedKey_PurgesPermissionsAndRestrictions()
    {
        _engine.Execute("create apikey 'purge-check'", "_system");
        _engine.Execute("grant writer on shop to 'purge-check'", "_system");
        _engine.Execute("restrict orders to reader for 'purge-check' on shop", "_system");

        _engine.Execute("purge apikey 'purge-check'", "_system");

        Assert.False(_engine.AuthService!.KeyExists("purge-check"));
    }

    [Fact]
    public void AuthService_RevokeAlsoRemovesRestrictions()
    {
        var create = _engine.Execute("create apikey 'rev-rest'", "_system");
        var apiKey = create.Data![0]["api_key"]!.ToString()!;

        _engine.Execute("grant writer on shop to 'rev-rest'", "_system");
        _engine.Execute("restrict orders to reader for 'rev-rest' on shop", "_system");

        _engine.Execute("revoke shop from 'rev-rest'", "_system");

        var entry = _engine.AuthService!.ValidateKey(apiKey);
        Assert.NotNull(entry);
        Assert.Empty(entry.Permissions);
        Assert.Empty(entry.Restrictions);
    }
}

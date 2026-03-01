using System.Net;
using System.Net.Http.Json;
using System.Text;
using System.Text.Json;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.TestHost;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using SproutDB.Core.DependencyInjection;
using SproutDB.Core.Server;

namespace SproutDB.Core.Tests.Auth;

public sealed class AuthEndpointTests : IAsyncLifetime
{
    private readonly string _dataDir = Path.Combine(Path.GetTempPath(), $"sproutdb-auth-http-{Guid.NewGuid()}");
    private const string MasterKey = "sdb_ak_testmasterkey1234567890abcdef12";
    private IHost? _host;
    private HttpClient? _client;

    public async Task InitializeAsync()
    {
        var builder = new HostBuilder()
            .ConfigureWebHost(webHost =>
            {
                webHost.UseTestServer();
                webHost.ConfigureServices(services =>
                {
                    services.AddSproutDB(options =>
                    {
                        options.DataDirectory = _dataDir;
                    });
                    services.AddSproutDBAuth(options =>
                    {
                        options.MasterKey = MasterKey;
                    });
                    services.AddRouting();
                });
                webHost.Configure(app =>
                {
                    app.UseRouting();
                    app.UseEndpoints(endpoints =>
                    {
                        endpoints.MapSproutDB();
                    });
                });
            });

        _host = await builder.StartAsync();
        _client = _host.GetTestClient();
    }

    public async Task DisposeAsync()
    {
        _client?.Dispose();

        if (_host is not null)
        {
            var engine = _host.Services.GetRequiredService<SproutEngine>();
            engine.Dispose();
            await _host.StopAsync();
            _host.Dispose();
        }

        if (Directory.Exists(_dataDir))
            Directory.Delete(_dataDir, true);
    }

    private HttpClient Client => _client ?? throw new InvalidOperationException("Test not initialized");

    private static readonly JsonSerializerOptions JsonOptions = new()
    {
        PropertyNamingPolicy = JsonNamingPolicy.SnakeCaseLower,
    };

    private async Task<(HttpResponseMessage Response, SproutResponse? Body)> PostQuery(
        string query, string? database = null, string? apiKey = null)
    {
        var request = new HttpRequestMessage(HttpMethod.Post, "/sproutdb/query")
        {
            Content = new StringContent(query, Encoding.UTF8, "text/plain"),
        };

        if (database is not null)
            request.Headers.Add("X-SproutDB-Database", database);

        if (apiKey is not null)
            request.Headers.Add("X-SproutDB-ApiKey", apiKey);

        var response = await Client.SendAsync(request);
        var body = await response.Content.ReadFromJsonAsync<SproutResponse>(JsonOptions);
        return (response, body);
    }

    // ── Auth required ──────────────────────────────────────

    [Fact]
    public async Task MissingApiKey_Returns401()
    {
        var (response, body) = await PostQuery("get users", database: "testdb");

        Assert.Equal(HttpStatusCode.Unauthorized, response.StatusCode);
        Assert.NotNull(body?.Errors);
        Assert.Contains(body.Errors, e => e.Code == "AUTH_REQUIRED");
    }

    [Fact]
    public async Task InvalidApiKey_Returns401()
    {
        var (response, body) = await PostQuery("get users", database: "testdb",
            apiKey: "sdb_ak_invalid123456789012345678901");

        Assert.Equal(HttpStatusCode.Unauthorized, response.StatusCode);
        Assert.NotNull(body?.Errors);
        Assert.Contains(body.Errors, e => e.Code == "AUTH_INVALID");
    }

    // ── Master key operations ──────────────────────────────

    [Fact]
    public async Task MasterKey_CanCreateApiKey()
    {
        var (response, body) = await PostQuery("create apikey 'http-key'", apiKey: MasterKey);

        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        Assert.NotNull(body);
        Assert.Null(body.Errors);
        Assert.NotNull(body.Data);
        Assert.Equal("http-key", body.Data[0]["name"]?.ToString());
        Assert.NotNull(body.Data[0]["api_key"]);
    }

    [Fact]
    public async Task MasterKey_CanCreateDatabaseWithHeader()
    {
        var (response, body) = await PostQuery("create database",
            database: "authdb", apiKey: MasterKey);

        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        Assert.Null(body?.Errors);
    }

    [Fact]
    public async Task AuthQuery_DoesNotRequireDatabaseHeader()
    {
        var (response, body) = await PostQuery("create apikey 'no-header-key'", apiKey: MasterKey);

        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        Assert.Null(body?.Errors);
    }

    [Fact]
    public async Task NormalQuery_RequiresDatabaseHeader()
    {
        var request = new HttpRequestMessage(HttpMethod.Post, "/sproutdb/query")
        {
            Content = new StringContent("get users", Encoding.UTF8, "text/plain"),
        };
        request.Headers.Add("X-SproutDB-ApiKey", MasterKey);

        var response = await Client.SendAsync(request);

        Assert.Equal(HttpStatusCode.BadRequest, response.StatusCode);
    }

    // ── Key-based access ───────────────────────────────────

    [Fact]
    public async Task CreatedKey_WithGrant_CanQuery()
    {
        // Create key + grant
        var create = await PostQuery("create apikey 'query-key'", apiKey: MasterKey);
        var keyValue = create.Body!.Data![0]["api_key"]!.ToString()!;

        await PostQuery("grant writer on testdb to 'query-key'", apiKey: MasterKey);
        await PostQuery("create database", database: "testdb", apiKey: MasterKey);
        await PostQuery("create table items (name string 100)", database: "testdb", apiKey: MasterKey);

        // Use created key
        var (response, body) = await PostQuery("get items", database: "testdb", apiKey: keyValue);

        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        Assert.Null(body?.Errors);
    }

    [Fact]
    public async Task CreatedKey_WithoutGrant_PermissionDenied()
    {
        var create = await PostQuery("create apikey 'nogrant-key'", apiKey: MasterKey);
        var keyValue = create.Body!.Data![0]["api_key"]!.ToString()!;

        await PostQuery("create database", database: "noaccess", apiKey: MasterKey);

        var (response, body) = await PostQuery("get users", database: "noaccess", apiKey: keyValue);

        Assert.Equal(HttpStatusCode.Forbidden, response.StatusCode);
        Assert.NotNull(body?.Errors);
        Assert.Contains(body.Errors, e => e.Code == "PERMISSION_DENIED");
    }

    // ── Key management requires master key ─────────────────

    [Fact]
    public async Task RegularKey_CannotCreateApiKey()
    {
        var create = await PostQuery("create apikey 'regular'", apiKey: MasterKey);
        var keyValue = create.Body!.Data![0]["api_key"]!.ToString()!;

        var (response, body) = await PostQuery("create apikey 'unauthorized'", apiKey: keyValue);

        Assert.Equal(HttpStatusCode.Forbidden, response.StatusCode);
        Assert.NotNull(body?.Errors);
        Assert.Contains(body.Errors, e => e.Code == "PERMISSION_DENIED");
    }

    // ── Rotate via HTTP ────────────────────────────────────

    [Fact]
    public async Task RotateApiKey_ReturnsNewKey()
    {
        var create = await PostQuery("create apikey 'rot-http'", apiKey: MasterKey);
        var oldKey = create.Body!.Data![0]["api_key"]!.ToString()!;

        var (response, body) = await PostQuery("rotate apikey 'rot-http'", apiKey: MasterKey);

        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        Assert.NotNull(body?.Data);
        var newKey = body.Data[0]["api_key"]!.ToString()!;
        Assert.NotEqual(oldKey, newKey);
        Assert.StartsWith("sdb_ak_", newKey);
    }

    // ── Duplicate key ──────────────────────────────────────

    [Fact]
    public async Task CreateApiKey_Duplicate_Returns409()
    {
        await PostQuery("create apikey 'dup-http'", apiKey: MasterKey);
        var (response, body) = await PostQuery("create apikey 'dup-http'", apiKey: MasterKey);

        Assert.Equal(HttpStatusCode.Conflict, response.StatusCode);
        Assert.NotNull(body?.Errors);
        Assert.Contains(body.Errors, e => e.Code == "KEY_EXISTS");
    }

    // ── Purge nonexistent ──────────────────────────────────

    [Fact]
    public async Task PurgeApiKey_NotFound_Returns404()
    {
        var (response, body) = await PostQuery("purge apikey 'ghost'", apiKey: MasterKey);

        Assert.Equal(HttpStatusCode.NotFound, response.StatusCode);
        Assert.NotNull(body?.Errors);
        Assert.Contains(body.Errors, e => e.Code == "KEY_NOT_FOUND");
    }
}

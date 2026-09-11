using System.Net;
using System.Net.Http.Json;
using System.Text;
using System.Text.Json;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.TestHost;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using SproutDB.Core;
using SproutDB.Core.DependencyInjection;
using SproutDB.Core.Server;

namespace SproutDB.Core.Tests.Server;

public sealed class HttpEndpointTests : IAsyncLifetime
{
    private readonly string _dataDir = Path.Combine(Path.GetTempPath(), $"sproutdb-http-{Guid.NewGuid()}");
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
        string query, string database = "testdb")
    {
        var request = new HttpRequestMessage(HttpMethod.Post, "/sproutdb/query")
        {
            Content = new StringContent(query, Encoding.UTF8, "text/plain"),
        };
        request.Headers.Add("X-SproutDB-Database", database);

        var response = await Client.SendAsync(request);
        var list = await response.Content.ReadFromJsonAsync<List<SproutResponse>>(JsonOptions);
        var body = list is { Count: > 0 } ? list[0] : null;
        return (response, body);
    }

    [Fact]
    public async Task MissingDatabaseHeader_Returns400()
    {
        var request = new HttpRequestMessage(HttpMethod.Post, "/sproutdb/query")
        {
            Content = new StringContent("get users", Encoding.UTF8, "text/plain"),
        };

        var response = await Client.SendAsync(request);

        Assert.Equal(HttpStatusCode.BadRequest, response.StatusCode);
    }

    [Fact]
    public async Task EmptyBody_Returns400()
    {
        var request = new HttpRequestMessage(HttpMethod.Post, "/sproutdb/query")
        {
            Content = new StringContent("", Encoding.UTF8, "text/plain"),
        };
        request.Headers.Add("X-SproutDB-Database", "testdb");

        var response = await Client.SendAsync(request);

        Assert.Equal(HttpStatusCode.BadRequest, response.StatusCode);
    }

    [Fact]
    public async Task CreateDatabase_Returns200()
    {
        var (response, body) = await PostQuery("create database");

        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        Assert.NotNull(body);
        Assert.Null(body.Errors);
    }

    [Fact]
    public async Task CreateDatabaseTwice_Returns409()
    {
        await PostQuery("create database", "conflictdb");
        var (response, body) = await PostQuery("create database", "conflictdb");

        // Multi-query: always 200, errors in individual responses
        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        Assert.NotNull(body?.Errors);
        Assert.Contains(body.Errors, e => e.Code == "DATABASE_EXISTS");
    }

    private async Task<(HttpResponseMessage Response, List<SproutResponse>? Body)> PostJson(
        string json, string database = "testdb")
    {
        var request = new HttpRequestMessage(HttpMethod.Post, "/sproutdb/query")
        {
            Content = new StringContent(json, Encoding.UTF8, "application/json"),
        };
        request.Headers.Add("X-SproutDB-Database", database);

        var response = await Client.SendAsync(request);
        if (response.StatusCode != HttpStatusCode.OK)
            return (response, null);
        return (response, await response.Content.ReadFromJsonAsync<List<SproutResponse>>(JsonOptions));
    }

    [Fact]
    public async Task JsonBody_WithParameters_BindsValues()
    {
        await PostQuery("create database");
        await PostQuery("create table gs (k string 64, n slong, ok bool)");

        var (_, insert) = await PostJson("""
            {"query": "upsert gs {k: @k, n: @n, ok: @ok}", "parameters": {"k": "x'; purge table gs; ##", "n": -5, "ok": true}}
            """);
        Assert.Null(insert?[0].Errors);

        var (response, get) = await PostJson("""
            {"query": "get gs select k, n where k in @keys", "parameters": {"keys": ["x'; purge table gs; ##", "other"]}}
            """);

        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        var row = Assert.Single(get?[0].Data ?? []);
        Assert.Equal("x'; purge table gs; ##", row["k"]?.ToString());
        Assert.Equal("-5", row["n"]?.ToString());
    }

    [Fact]
    public async Task JsonBody_WithoutParameters_Works()
    {
        await PostQuery("create database");

        var (response, body) = await PostJson("""{"query": "create table t (k string 10)"}""");

        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        Assert.Null(body?[0].Errors);
    }

    [Fact]
    public async Task JsonBody_MissingParameter_ReturnsParameterError()
    {
        await PostQuery("create database");
        await PostQuery("create table gs (k string 64)");

        var (response, body) = await PostJson("""{"query": "get gs where k = @key", "parameters": {}}""");

        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        Assert.Equal("PARAMETER_ERROR", body?[0].Errors?[0].Code);
    }

    [Theory]
    [InlineData("not json")]
    [InlineData("""{"parameters": {}}""")]
    [InlineData("""{"query": "get gs", "parameters": [1]}""")]
    [InlineData("""{"query": "get gs where k = @k", "parameters": {"k": {"nested": 1}}}""")]
    public async Task JsonBody_Invalid_Returns400(string json)
    {
        var (response, _) = await PostJson(json);
        Assert.Equal(HttpStatusCode.BadRequest, response.StatusCode);
    }

    [Fact]
    public async Task ConditionalUpsert_ConcurrentClients_ExactlyOneWins()
    {
        await PostQuery("create database");
        await PostQuery("create table gs (k string 64, etag string 16)");
        await PostQuery("create index unique gs.k");
        await PostQuery("upsert gs {k: 'a', etag: 'E1'}");

        var host = _host ?? throw new InvalidOperationException("Test not initialized");
        var clients = Enumerable.Range(0, 6).Select(_ => host.GetTestClient()).ToList();

        var results = await Task.WhenAll(clients.Select(async (client, i) =>
        {
            var request = new HttpRequestMessage(HttpMethod.Post, "/sproutdb/query")
            {
                Content = new StringContent(
                    $"upsert gs {{k: 'a', etag: 'W{i}'}} on k when etag = 'E1'", Encoding.UTF8, "text/plain"),
            };
            request.Headers.Add("X-SproutDB-Database", "testdb");
            var response = await client.SendAsync(request);
            var list = await response.Content.ReadFromJsonAsync<List<SproutResponse>>(JsonOptions);
            return list is { Count: > 0 } ? list[0] : null;
        }));

        foreach (var client in clients)
            client.Dispose();

        Assert.Equal(1, results.Count(r => r is not null && r.Errors is null));
        Assert.Equal(5, results.Count(r => r?.Errors?[0].Code == "CONDITION_FAILED"));

        // Losers get the stored row, incl. the winner's etag
        var winnerEtag = results.First(r => r is not null && r.Errors is null)?.Data?[0]["etag"]?.ToString();
        var loser = results.First(r => r?.Errors is not null);
        Assert.Equal(winnerEtag, loser?.Data?[0]["etag"]?.ToString());
    }

    [Fact]
    public async Task UnknownDatabase_ReturnsError()
    {
        var (response, body) = await PostQuery("get users", "nonexistent");

        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        Assert.NotNull(body?.Errors);
        Assert.Contains(body.Errors, e => e.Code == "UNKNOWN_DATABASE");
    }

    [Fact]
    public async Task UnknownTable_ReturnsError()
    {
        await PostQuery("create database", "tabletest");
        var (response, body) = await PostQuery("get nonexistent", "tabletest");

        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        Assert.NotNull(body?.Errors);
        Assert.Contains(body.Errors, e => e.Code == "UNKNOWN_TABLE");
    }

    [Fact]
    public async Task SyntaxError_ReturnsError()
    {
        await PostQuery("create database", "syntaxdb");
        var (response, body) = await PostQuery("this is not valid", "syntaxdb");

        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        Assert.NotNull(body?.Errors);
        Assert.Contains(body.Errors, e => e.Code == "SYNTAX_ERROR");
    }

    [Fact]
    public async Task ProtectedName_ReturnsError()
    {
        var (response, body) = await PostQuery("create database", "_protected");

        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        Assert.NotNull(body?.Errors);
        Assert.Contains(body.Errors, e => e.Code == "PROTECTED_NAME");
    }

    [Fact]
    public async Task CreateTableAndUpsertAndGet_Returns200WithData()
    {
        await PostQuery("create database", "cruddb");
        await PostQuery("create table users (name string 100, age ubyte)", "cruddb");

        var (upsertResp, upsertBody) = await PostQuery(
            "upsert users {name: 'Alice', age: 30}", "cruddb");
        Assert.Equal(HttpStatusCode.OK, upsertResp.StatusCode);
        Assert.NotNull(upsertBody);
        Assert.Null(upsertBody.Errors);
        Assert.Equal(1, upsertBody.Affected);

        var (getResp, getBody) = await PostQuery("get users", "cruddb");
        Assert.Equal(HttpStatusCode.OK, getResp.StatusCode);
        Assert.NotNull(getBody?.Data);
        Assert.Single(getBody.Data);
    }

    [Fact]
    public async Task ResponseUsesSnakeCaseJson()
    {
        await PostQuery("create database", "jsondb");
        await PostQuery("create table items (value string 50)", "jsondb");
        await PostQuery("upsert items {value: 'test'}", "jsondb");

        var request = new HttpRequestMessage(HttpMethod.Post, "/sproutdb/query")
        {
            Content = new StringContent("get items", Encoding.UTF8, "text/plain"),
        };
        request.Headers.Add("X-SproutDB-Database", "jsondb");

        var response = await Client.SendAsync(request);
        var rawJson = await response.Content.ReadAsStringAsync();

        Assert.Contains("\"annotated_query\"", rawJson);
        Assert.DoesNotContain("\"AnnotatedQuery\"", rawJson);
    }

    [Fact]
    public async Task IndexExists_ReturnsError()
    {
        await PostQuery("create database", "idxdb");
        await PostQuery("create table products (name string 100)", "idxdb");
        await PostQuery("create index products.name", "idxdb");

        var (response, body) = await PostQuery("create index products.name", "idxdb");

        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        Assert.NotNull(body?.Errors);
        Assert.Contains(body.Errors, e => e.Code == "INDEX_EXISTS");
    }

    [Fact]
    public async Task IndexNotFound_ReturnsError()
    {
        await PostQuery("create database", "purgeidxdb");
        await PostQuery("create table items (name string 100)", "purgeidxdb");

        var (response, body) = await PostQuery("purge index items.name", "purgeidxdb");

        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        Assert.NotNull(body?.Errors);
        Assert.Contains(body.Errors, e => e.Code == "INDEX_NOT_FOUND");
    }

    [Fact]
    public async Task ResponseIsAlwaysArray()
    {
        await PostQuery("create database", "arraydb");

        var request = new HttpRequestMessage(HttpMethod.Post, "/sproutdb/query")
        {
            Content = new StringContent("describe", Encoding.UTF8, "text/plain"),
        };
        request.Headers.Add("X-SproutDB-Database", "arraydb");

        var response = await Client.SendAsync(request);
        var rawJson = await response.Content.ReadAsStringAsync();

        Assert.StartsWith("[", rawJson.TrimStart());
    }
}

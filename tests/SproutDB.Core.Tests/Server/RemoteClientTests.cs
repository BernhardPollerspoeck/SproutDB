using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Http.Connections;
using Microsoft.AspNetCore.TestHost;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using SproutDB.Core.Client;
using SproutDB.Core.DependencyInjection;
using SproutDB.Core.Server;

namespace SproutDB.Core.Tests.Server;

/// <summary>
/// <see cref="SproutClient"/> against a real server (TestServer): the same
/// ISproutServer / ISproutDatabase code must work remotely.
/// </summary>
public sealed class RemoteClientTests : IAsyncLifetime
{
    private sealed class Grain : ISproutEntity
    {
        public ulong Id { get; set; }
        public string K { get; set; } = "";
        public string? Etag { get; set; }
        public byte Level { get; set; }
    }

    private readonly string _dataDir = Path.Combine(Path.GetTempPath(), $"sproutdb-remote-{Guid.NewGuid()}");
    private IHost? _host;
    private SproutClient? _client;

    private const string MasterKey = "sdb_ak_testmasterkey000000000000000000000000";

    public Task InitializeAsync() => StartAsync(auth: false);

    private async Task StartAsync(bool auth)
    {
        var builder = new HostBuilder()
            .ConfigureWebHost(webHost =>
            {
                webHost.UseTestServer();
                webHost.ConfigureServices(services =>
                {
                    services.AddSproutDB(options => options.DataDirectory = _dataDir);
                    if (auth)
                        services.AddSproutDBAuth(o => o.MasterKey = MasterKey);
                    services.AddRouting();
                    services.AddSignalR();
                });
                webHost.Configure(app =>
                {
                    app.UseRouting();
                    app.UseEndpoints(endpoints =>
                    {
                        endpoints.MapSproutDB();
                        endpoints.MapSproutDBHub();
                    });
                });
            });

        _host = await builder.StartAsync();
        _client = CreateClient(auth ? MasterKey : null);
    }

    private SproutClient CreateClient(string? apiKey)
    {
        var host = _host ?? throw new InvalidOperationException("not started");
        var server = host.GetTestServer();
        return new SproutClient(server.CreateClient(), new SproutClientOptions
        {
            ApiKey = apiKey,
            // TestServer has no WebSockets — long polling through its in-memory handler
            ConfigureChangeConnection = o =>
            {
                o.HttpMessageHandlerFactory = _ => server.CreateHandler();
                o.Transports = HttpTransportType.LongPolling;
            },
        });
    }

    public async Task DisposeAsync()
    {
        _client?.Dispose();
        if (_host is not null)
        {
            _host.Services.GetRequiredService<SproutEngine>().Dispose();
            await _host.StopAsync();
            _host.Dispose();
        }
        if (Directory.Exists(_dataDir))
            Directory.Delete(_dataDir, true);
    }

    private SproutClient Client => _client ?? throw new InvalidOperationException("not started");

    private ISproutDatabase NewDb()
    {
        var db = Client.GetOrCreateDatabase("remote");
        db.Query("create table gs (k string 64 strict unique, etag string 16, level ubyte)");
        return db;
    }

    // ── Queries ──────────────────────────────────────────────

    [Fact]
    public void Query_RoundTrip_IdIsUlong()
    {
        var db = NewDb();

        var upsert = db.Query("upsert gs {k: 'a', etag: 'E1', level: 3}")[0];
        Assert.Null(upsert.Errors);

        var row = Assert.Single(db.Query("get gs")[0].Data ?? []);
        Assert.Equal(1UL, row["_id"]);
        Assert.Equal("a", row["k"]);
        Assert.Equal(3L, row["level"]);
    }

    [Fact]
    public void Query_Error_IsReturnedNotThrown()
    {
        var db = NewDb();

        var result = db.Query("get nope")[0];

        Assert.Equal("UNKNOWN_TABLE", result.Errors?[0].Code);
    }

    [Fact]
    public void GetOrCreateDatabase_Twice_DoesNotThrow()
    {
        Client.GetOrCreateDatabase("twice");
        var db = Client.GetOrCreateDatabase("twice");
        Assert.Equal("twice", db.Name);
    }

    [Fact]
    public void SelectDatabase_ExistingAndMissing()
    {
        Client.GetOrCreateDatabase("present");

        Assert.Equal("present", Client.SelectDatabase("present").Name);
        Assert.Throws<InvalidOperationException>(() => Client.SelectDatabase("absent"));
    }

    [Fact]
    public void ConditionalUpsert_ReturnsCurrentRow()
    {
        var db = NewDb();
        db.Query("upsert gs {k: 'a', etag: 'E5'}");

        var stale = db.Query("upsert gs {k: 'a', etag: 'E2'} on k when etag = 'E1'")[0];

        Assert.Equal("CONDITION_FAILED", stale.Errors?[0].Code);
        Assert.Equal("E5", Assert.Single(stale.Data ?? [])["etag"]);
    }

    [Fact]
    public void Parameters_BindClientSide()
    {
        var db = NewDb();
        const string hostile = "x'; purge table gs; ##";

        db.Query("upsert gs {k: @k, etag: @e}", new { k = hostile, e = "E1" });
        var r = db.Query("get gs select etag where k = @k", new { k = hostile })[0];

        Assert.Equal("E1", Assert.Single(r.Data ?? [])["etag"]);
    }

    [Fact]
    public void TypedApi_WorksRemotely()
    {
        var db = NewDb();
        var grains = db.Table<Grain>("gs");

        grains.Upsert(new Grain { K = "a", Etag = "E1", Level = 2 });
        grains.Upsert(new Grain { K = "b", Etag = "E1", Level = 7 });

        var high = grains.Where(g => g.Level > 5).ToList();
        var single = Assert.Single(high);
        Assert.Equal("b", single.K);
        Assert.Equal(2UL, single.Id);
        Assert.Equal(2, db.Table<Grain>("gs").Count()); // SproutTable is a mutable builder — fresh one

        var cas = grains.Upsert(new Grain { K = "a", Etag = "E2", Level = 2 }, on: g => g.K, when: g => g.Etag == "E1");
        Assert.Null(cas.Errors);
    }

    // ── Async ────────────────────────────────────────────────

    [Fact]
    public async Task QueryAsync_RoundTrip()
    {
        var db = NewDb();

        await db.QueryAsync("upsert gs {k: @k}", new { k = "a" });
        var results = await db.QueryAsync("get gs");

        Assert.Single(results[0].Data ?? []);
    }

    [Fact]
    public async Task QueryAsync_CancelledBeforeSend_WritesNothing()
    {
        var db = NewDb();
        using var cts = new CancellationTokenSource();
        cts.Cancel();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(
            async () => await db.QueryAsync("upsert gs {k: 'a'}", cts.Token));
        Assert.Empty(db.Query("get gs")[0].Data ?? [new Dictionary<string, object?>()]);
    }

    // ── OnChange (SignalR) ───────────────────────────────────

    [Fact]
    public void OnChange_ReceivesChangesOfSubscribedTableOnly()
    {
        var db = NewDb();
        db.Query("create table other (v sint)");

        var received = new List<SproutResponse>();
        using var signal = new ManualResetEventSlim();
        using (db.OnChange("gs", r => { lock (received) received.Add(r); signal.Set(); }))
        {
            db.Query("upsert other {v: 1}");
            db.Query("upsert gs {k: 'a', etag: 'E1'}");

            Assert.True(signal.Wait(TimeSpan.FromSeconds(10)), "no change notification received");
        }

        lock (received)
        {
            var change = Assert.Single(received);
            Assert.Equal(SproutOperation.Upsert, change.Operation);
            Assert.Equal("a", change.Data?[0]["k"]);
        }
    }

    // ── Not available remotely ───────────────────────────────

    [Fact]
    public void ServerOnlyOperations_AreNotSupported()
    {
        var db = NewDb();

        Assert.Throws<NotSupportedException>(() => Client.GetDatabases());
        Assert.Throws<NotSupportedException>(() => Client.Migrate(typeof(RemoteClientTests).Assembly, db));
        Assert.Throws<NotSupportedException>(() => db.SaveQuery("q", "get gs"));
    }

    // ── DI + auth ────────────────────────────────────────────

    [Fact]
    public void AddSproutDBClient_RegistersClientAsSproutServer()
    {
        var services = new ServiceCollection();
        services.AddSproutDBClient(o => o.BaseAddress = new Uri("http://localhost:5000/"));
        using var provider = services.BuildServiceProvider();

        var server = provider.GetRequiredService<ISproutServer>();
        Assert.IsType<SproutClient>(server);
        Assert.Same(provider.GetRequiredService<SproutClient>(), server);
    }

    [Fact]
    public async Task Auth_ApiKeyIsSent()
    {
        await DisposeAsync();
        await StartAsync(auth: true);

        var db = Client.GetOrCreateDatabase("secured");
        Assert.Null(db.Query("create table t (v sint)")[0].Errors);

        using var anonymous = CreateClient(apiKey: null);
        var denied = anonymous.SelectDatabaseOrError("secured");
        Assert.Equal("AUTH_REQUIRED", denied);
    }
}

internal static class RemoteClientTestExtensions
{
    /// <summary>Error code of a plain query against <paramref name="database"/>, or null.</summary>
    public static string? SelectDatabaseOrError(this SproutClient client, string database)
    {
        try
        {
            client.SelectDatabase(database);
            return null;
        }
        catch (SproutQueryException ex) when (ex.Message.Contains("X-SproutDB-ApiKey"))
        {
            return "AUTH_REQUIRED";
        }
    }
}

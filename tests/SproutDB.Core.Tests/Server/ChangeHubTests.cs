using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.SignalR.Client;
using Microsoft.AspNetCore.TestHost;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using SproutDB.Core.DependencyInjection;
using SproutDB.Core.Server;

namespace SproutDB.Core.Tests.Server;

public sealed class ChangeHubTests : IAsyncLifetime
{
    private readonly string _dataDir = Path.Combine(Path.GetTempPath(), $"sproutdb-hub-{Guid.NewGuid()}");
    private IHost? _host;

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
                    services.AddSignalR();
                    services.AddRouting();
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

        // Create test database + table
        var engine = _host.Services.GetRequiredService<SproutEngine>();
        engine.Execute("create database", "testdb");
        engine.Execute("create table users (name string 100)", "testdb");
    }

    public async Task DisposeAsync()
    {
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

    private HubConnection CreateHubConnection()
    {
        var server = _host?.GetTestServer() ?? throw new InvalidOperationException("Test not initialized");
        return new HubConnectionBuilder()
            .WithUrl(
                "http://localhost/sproutdb/changes",
                options =>
                {
                    options.HttpMessageHandlerFactory = _ => server.CreateHandler();
                })
            .Build();
    }

    [Fact]
    public async Task Client_CanConnect()
    {
        var connection = CreateHubConnection();
        await connection.StartAsync();

        Assert.Equal(HubConnectionState.Connected, connection.State);

        await connection.DisposeAsync();
    }

    [Fact]
    public async Task Subscribe_ReceivesUpsertNotification()
    {
        var connection = CreateHubConnection();
        await connection.StartAsync();

        var received = new List<object?>();
        var tcs = new TaskCompletionSource<bool>();
        connection.On<object>("OnChange", response =>
        {
            received.Add(response);
            tcs.TrySetResult(true);
        });

        await connection.InvokeAsync("Subscribe", "testdb", "users");

        var engine = _host?.Services.GetRequiredService<SproutEngine>()
            ?? throw new InvalidOperationException("Test not initialized");
        engine.Execute("upsert users {name: 'John'}", "testdb");

        // Wait for the notification with timeout
        var completed = await Task.WhenAny(tcs.Task, Task.Delay(3000));
        Assert.Same(tcs.Task, completed);
        Assert.Single(received);

        await connection.DisposeAsync();
    }

    [Fact]
    public async Task Unsubscribe_StopsNotifications()
    {
        var connection = CreateHubConnection();
        await connection.StartAsync();

        var received = new List<object?>();
        connection.On<object>("OnChange", response => received.Add(response));

        await connection.InvokeAsync("Subscribe", "testdb", "users");
        await connection.InvokeAsync("Unsubscribe", "testdb", "users");

        var engine = _host?.Services.GetRequiredService<SproutEngine>()
            ?? throw new InvalidOperationException("Test not initialized");
        engine.Execute("upsert users {name: 'John'}", "testdb");

        await Task.Delay(500);
        Assert.Empty(received);

        await connection.DisposeAsync();
    }

    [Fact]
    public async Task ReadOperation_DoesNotTriggerNotification()
    {
        var connection = CreateHubConnection();
        await connection.StartAsync();

        var received = new List<object?>();
        connection.On<object>("OnChange", response => received.Add(response));

        await connection.InvokeAsync("Subscribe", "testdb", "users");

        var engine = _host?.Services.GetRequiredService<SproutEngine>()
            ?? throw new InvalidOperationException("Test not initialized");
        engine.Execute("get users", "testdb");

        await Task.Delay(500);
        Assert.Empty(received);

        await connection.DisposeAsync();
    }
}

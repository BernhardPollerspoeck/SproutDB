using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using SproutDB.Core.Auth;
using SproutDB.Core.DependencyInjection;

namespace SproutDB.Core.Tests.DependencyInjection;

public class DiTests : IDisposable
{
    private readonly string _tempDir;

    public DiTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), $"sproutdb-di-{Guid.NewGuid()}");
    }

    public void Dispose()
    {
        if (Directory.Exists(_tempDir))
            Directory.Delete(_tempDir, true);
    }

    [Fact]
    public void AddSproutDB_RegistersEngine()
    {
        var services = new ServiceCollection();
        services.AddSproutDB(options =>
        {
            options.DataDirectory = _tempDir;
        });

        using var provider = services.BuildServiceProvider();
        var engine = provider.GetRequiredService<SproutEngine>();

        Assert.NotNull(engine);
    }

    [Fact]
    public void AddSproutDB_RegistersISproutServer()
    {
        var services = new ServiceCollection();
        services.AddSproutDB(options =>
        {
            options.DataDirectory = _tempDir;
        });

        using var provider = services.BuildServiceProvider();
        var server = provider.GetRequiredService<ISproutServer>();

        Assert.NotNull(server);
        Assert.IsType<SproutEngine>(server);
    }

    [Fact]
    public void AddSproutDB_SingletonSameInstance()
    {
        var services = new ServiceCollection();
        services.AddSproutDB(options =>
        {
            options.DataDirectory = _tempDir;
        });

        using var provider = services.BuildServiceProvider();
        var engine = provider.GetRequiredService<SproutEngine>();
        var server = provider.GetRequiredService<ISproutServer>();

        Assert.Same(engine, server);
    }

    [Fact]
    public void AddSproutDB_ConfiguresSettings()
    {
        var services = new ServiceCollection();
        services.AddSproutDB(options =>
        {
            options.DataDirectory = _tempDir;
            options.ChunkSize = 50_000;
            options.BulkLimit = 200;
        });

        using var provider = services.BuildServiceProvider();
        var settings = provider.GetRequiredService<SproutEngineSettings>();

        Assert.Equal(50_000, settings.ChunkSize);
        Assert.Equal(200, settings.BulkLimit);
    }

    [Fact]
    public void AddSproutDB_EngineWorks()
    {
        var services = new ServiceCollection();
        services.AddSproutDB(options =>
        {
            options.DataDirectory = _tempDir;
        });

        using var provider = services.BuildServiceProvider();
        var engine = provider.GetRequiredService<SproutEngine>();

        var result = engine.Execute("create database", "testdb");
        Assert.Equal(SproutOperation.CreateDatabase, result.Operation);
    }

    // ── IConfiguration overload ──────────────────────────────

    [Fact]
    public void AddSproutDB_FromConfiguration_BindsSettings()
    {
        var config = new ConfigurationBuilder()
            .AddInMemoryCollection(new Dictionary<string, string?>
            {
                ["SproutDB:DataDirectory"] = _tempDir,
                ["SproutDB:DefaultPageSize"] = "50",
                ["SproutDB:BulkLimit"] = "500",
                ["SproutDB:WalFlushIntervalSeconds"] = "10",
                ["SproutDB:WalSyncIntervalMs"] = "200",
                ["SproutDB:PreAllocateChunkSize"] = "20000",
            })
            .Build();

        var services = new ServiceCollection();
        services.AddSproutDB(config);

        using var provider = services.BuildServiceProvider();
        var settings = provider.GetRequiredService<SproutEngineSettings>();

        Assert.Equal(_tempDir, settings.DataDirectory);
        Assert.Equal(50, settings.DefaultPageSize);
        Assert.Equal(500, settings.BulkLimit);
        Assert.Equal(TimeSpan.FromSeconds(10), settings.FlushInterval);
        Assert.Equal(TimeSpan.FromMilliseconds(200), settings.WalSyncInterval);
        Assert.Equal(20_000, settings.ChunkSize);
    }

    [Fact]
    public void AddSproutDB_FromConfiguration_BindsAutoIndex()
    {
        var config = new ConfigurationBuilder()
            .AddInMemoryCollection(new Dictionary<string, string?>
            {
                ["SproutDB:DataDirectory"] = _tempDir,
                ["SproutDB:AutoIndex:Enabled"] = "false",
                ["SproutDB:AutoIndex:UsageThresholdPercent"] = "50",
                ["SproutDB:AutoIndex:SelectivityThresholdPercent"] = "80",
                ["SproutDB:AutoIndex:ReadWriteRatioThreshold"] = "5.0",
                ["SproutDB:AutoIndex:UnusedIndexRemovalDays"] = "60",
                ["SproutDB:AutoIndex:MinimumQueryCount"] = "200",
            })
            .Build();

        var services = new ServiceCollection();
        services.AddSproutDB(config);

        using var provider = services.BuildServiceProvider();
        var settings = provider.GetRequiredService<SproutEngineSettings>();

        Assert.False(settings.AutoIndex.Enabled);
        Assert.Equal(0.50, settings.AutoIndex.UsageThreshold);
        Assert.Equal(0.80, settings.AutoIndex.SelectivityThreshold);
        Assert.Equal(5.0, settings.AutoIndex.ReadWriteRatio);
        Assert.Equal(60, settings.AutoIndex.UnusedRetentionDays);
        Assert.Equal(200, settings.AutoIndex.MinimumQueryCount);
    }

    [Fact]
    public void AddSproutDB_FromConfiguration_BindsAuth()
    {
        var config = new ConfigurationBuilder()
            .AddInMemoryCollection(new Dictionary<string, string?>
            {
                ["SproutDB:DataDirectory"] = _tempDir,
                ["SproutDB:Auth:MasterKey"] = "sdb_ak_testkey12345678901234567890ab",
            })
            .Build();

        var services = new ServiceCollection();
        services.AddSproutDB(config);

        using var provider = services.BuildServiceProvider();
        var authOptions = provider.GetService<SproutAuthOptions>();

        Assert.NotNull(authOptions);
        Assert.Equal("sdb_ak_testkey12345678901234567890ab", authOptions.MasterKey);
    }

    [Fact]
    public void AddSproutDB_FromConfiguration_CodeOverrideWins()
    {
        var config = new ConfigurationBuilder()
            .AddInMemoryCollection(new Dictionary<string, string?>
            {
                ["SproutDB:DataDirectory"] = "/some/other/dir",
                ["SproutDB:DefaultPageSize"] = "50",
            })
            .Build();

        var services = new ServiceCollection();
        services.AddSproutDB(config, builder =>
        {
            builder.DataDirectory = _tempDir; // override config value
            builder.BulkLimit = 999;
        });

        using var provider = services.BuildServiceProvider();
        var settings = provider.GetRequiredService<SproutEngineSettings>();

        Assert.Equal(_tempDir, settings.DataDirectory); // code wins
        Assert.Equal(50, settings.DefaultPageSize); // from config
        Assert.Equal(999, settings.BulkLimit); // from code
    }

    [Fact]
    public void AddSproutDB_FromConfiguration_EngineWorks()
    {
        var config = new ConfigurationBuilder()
            .AddInMemoryCollection(new Dictionary<string, string?>
            {
                ["SproutDB:DataDirectory"] = _tempDir,
            })
            .Build();

        var services = new ServiceCollection();
        services.AddSproutDB(config);

        using var provider = services.BuildServiceProvider();
        var engine = provider.GetRequiredService<SproutEngine>();

        var result = engine.Execute("create database", "testdb");
        Assert.Equal(SproutOperation.CreateDatabase, result.Operation);
    }

    [Fact]
    public void AddSproutDB_FromConfiguration_MissingSection_UsesDefaults()
    {
        var config = new ConfigurationBuilder()
            .AddInMemoryCollection(new Dictionary<string, string?>
            {
                ["SproutDB:DataDirectory"] = _tempDir,
            })
            .Build();

        var services = new ServiceCollection();
        services.AddSproutDB(config);

        using var provider = services.BuildServiceProvider();
        var settings = provider.GetRequiredService<SproutEngineSettings>();

        Assert.Equal(100, settings.DefaultPageSize);
        Assert.Equal(100, settings.BulkLimit);
        Assert.True(settings.AutoIndex.Enabled);
        Assert.Null(settings.MasterKey);
    }
}

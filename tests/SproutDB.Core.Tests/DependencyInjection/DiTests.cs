using Microsoft.Extensions.DependencyInjection;
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
}

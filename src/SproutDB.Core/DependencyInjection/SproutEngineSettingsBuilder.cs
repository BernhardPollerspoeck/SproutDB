using System.Reflection;
using SproutDB.Core.AutoIndex;

namespace SproutDB.Core.DependencyInjection;

/// <summary>
/// Mutable builder for <see cref="SproutEngineSettings"/>.
/// Used by the DI extension to configure settings via an Action callback.
/// </summary>
public sealed class SproutEngineSettingsBuilder
{
    public string DataDirectory { get; set; } = "";
    public TimeSpan FlushInterval { get; set; } = TimeSpan.FromSeconds(5);
    public TimeSpan WalSyncInterval { get; set; } = TimeSpan.FromMilliseconds(50);
    public int BulkLimit { get; set; } = 100;
    public int DefaultPageSize { get; set; } = 100;
    public int ChunkSize { get; set; } = 10_000;
    public AutoIndexSettings AutoIndex { get; set; } = new();
    public List<(Assembly Assembly, string Database)> Migrations { get; } = [];

    public void AddMigrations<TMarker>(string database)
        => Migrations.Add((typeof(TMarker).Assembly, database));

    internal SproutEngineSettings Build() => new()
    {
        DataDirectory = DataDirectory,
        FlushInterval = FlushInterval,
        WalSyncInterval = WalSyncInterval,
        BulkLimit = BulkLimit,
        DefaultPageSize = DefaultPageSize,
        ChunkSize = ChunkSize,
        AutoIndex = AutoIndex,
    };
}

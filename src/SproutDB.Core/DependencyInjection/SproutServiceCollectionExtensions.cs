using System.Globalization;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using SproutDB.Core.Auth;
using SproutDB.Core.AutoIndex;
using SproutDB.Core.Server;

namespace SproutDB.Core.DependencyInjection;

/// <summary>
/// Extension methods for registering SproutDB in a DI container.
/// </summary>
public static class SproutServiceCollectionExtensions
{
    /// <summary>
    /// Registers SproutDB from an <see cref="IConfiguration"/> section named "SproutDB".
    /// Supports appsettings.json, environment variables, user-secrets, etc.
    /// </summary>
    public static IServiceCollection AddSproutDB(
        this IServiceCollection services,
        IConfiguration configuration)
    {
        return AddSproutDB(services, configuration, null);
    }

    /// <summary>
    /// Registers SproutDB from an <see cref="IConfiguration"/> section named "SproutDB",
    /// with optional code overrides applied after configuration binding.
    /// </summary>
    public static IServiceCollection AddSproutDB(
        this IServiceCollection services,
        IConfiguration configuration,
        Action<SproutEngineSettingsBuilder>? configure)
    {
        var builder = new SproutEngineSettingsBuilder();
        BindConfiguration(builder, configuration.GetSection("SproutDB"));
        configure?.Invoke(builder);
        return RegisterEngine(services, builder);
    }

    /// <summary>
    /// Registers a <see cref="SproutEngine"/> singleton and its interfaces
    /// (<see cref="ISproutServer"/>) in the service collection.
    /// </summary>
    public static IServiceCollection AddSproutDB(
        this IServiceCollection services,
        Action<SproutEngineSettingsBuilder> configure)
    {
        var builder = new SproutEngineSettingsBuilder();
        configure(builder);
        return RegisterEngine(services, builder);
    }

    /// <summary>
    /// Enables authentication for SproutDB.
    /// Must be called after <see cref="AddSproutDB"/>.
    /// </summary>
    public static IServiceCollection AddSproutDBAuth(
        this IServiceCollection services,
        Action<SproutAuthOptions> configure)
    {
        var options = new SproutAuthOptions { MasterKey = "" };
        configure(options);
        services.AddSingleton(options);
        return services;
    }

    // ── Internal helpers ─────────────────────────────────────

    private static IServiceCollection RegisterEngine(
        IServiceCollection services,
        SproutEngineSettingsBuilder builder)
    {
        var settings = builder.Build();

        services.AddSingleton(settings);
        services.AddSingleton(sp =>
        {
            var authOptions = sp.GetService<SproutAuthOptions>();
            return new SproutEngine(sp.GetRequiredService<SproutEngineSettings>(), authOptions);
        });
        services.AddSingleton<ISproutServer>(sp => sp.GetRequiredService<SproutEngine>());

        if (builder.Migrations.Count > 0)
        {
            services.Configure<SproutMigrationOptions>(options =>
            {
                foreach (var migration in builder.Migrations)
                    options.Migrations.Add(migration);
            });
            services.AddHostedService<SproutMigrationHostedService>();
        }

        // Auto-register auth if MasterKey is set via configuration
        if (builder.MasterKey is not null)
        {
            var masterKey = builder.MasterKey;
            services.AddSingleton(new SproutAuthOptions { MasterKey = masterKey });
        }

        return services;
    }

    /// <summary>
    /// Maps the "SproutDB" configuration section to the builder.
    /// JSON keys follow the design doc (sproutdb-api-reference.md).
    /// </summary>
    private static void BindConfiguration(SproutEngineSettingsBuilder builder, IConfigurationSection section)
    {
        if (section["DataDirectory"] is { } dataDir)
            builder.DataDirectory = dataDir;

        if (int.TryParse(section["DefaultPageSize"], out var pageSize))
            builder.DefaultPageSize = pageSize;

        if (int.TryParse(section["BulkLimit"], out var bulkLimit))
            builder.BulkLimit = bulkLimit;

        if (int.TryParse(section["WalFlushIntervalSeconds"], out var flushSec))
            builder.FlushInterval = TimeSpan.FromSeconds(flushSec);

        if (int.TryParse(section["WalSyncIntervalMs"], out var syncMs))
            builder.WalSyncInterval = TimeSpan.FromMilliseconds(syncMs);

        if (int.TryParse(section["PreAllocateChunkSize"], out var chunkSize))
            builder.ChunkSize = chunkSize;

        // AutoIndex sub-section
        var autoIndex = section.GetSection("AutoIndex");
        if (autoIndex.Exists())
        {
            var ai = new AutoIndexSettings();
            var enabled = ai.Enabled;
            var usage = ai.UsageThreshold;
            var selectivity = ai.SelectivityThreshold;
            var rwRatio = ai.ReadWriteRatio;
            var retention = ai.UnusedRetentionDays;
            var minQueries = ai.MinimumQueryCount;

            if (bool.TryParse(autoIndex["Enabled"], out var e))
                enabled = e;
            if (int.TryParse(autoIndex["UsageThresholdPercent"], out var u))
                usage = u / 100.0;
            if (int.TryParse(autoIndex["SelectivityThresholdPercent"], out var s))
                selectivity = s / 100.0;
            if (double.TryParse(autoIndex["ReadWriteRatioThreshold"], NumberStyles.Float, CultureInfo.InvariantCulture, out var rw))
                rwRatio = rw;
            if (int.TryParse(autoIndex["UnusedIndexRemovalDays"], out var ret))
                retention = ret;
            if (int.TryParse(autoIndex["MinimumQueryCount"], out var mq))
                minQueries = mq;

            builder.AutoIndex = new AutoIndexSettings
            {
                Enabled = enabled,
                UsageThreshold = usage,
                SelectivityThreshold = selectivity,
                ReadWriteRatio = rwRatio,
                UnusedRetentionDays = retention,
                MinimumQueryCount = minQueries,
            };
        }

        // Auth sub-section
        var auth = section.GetSection("Auth");
        if (auth["MasterKey"] is { } masterKey)
            builder.MasterKey = masterKey;
    }
}

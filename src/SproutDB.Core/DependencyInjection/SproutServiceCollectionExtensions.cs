using Microsoft.Extensions.DependencyInjection;
using SproutDB.Core.Server;

namespace SproutDB.Core.DependencyInjection;

/// <summary>
/// Extension methods for registering SproutDB in a DI container.
/// </summary>
public static class SproutServiceCollectionExtensions
{
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
        var settings = builder.Build();

        services.AddSingleton(settings);
        services.AddSingleton<SproutEngine>();
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

        return services;
    }
}

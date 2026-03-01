using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Options;

namespace SproutDB.Core.Server;

internal sealed class SproutMigrationHostedService : IHostedService
{
    private readonly SproutEngine _engine;
    private readonly SproutMigrationOptions _options;

    public SproutMigrationHostedService(SproutEngine engine, IOptions<SproutMigrationOptions> options)
    {
        _engine = engine;
        _options = options.Value;
    }

    public Task StartAsync(CancellationToken cancellationToken)
    {
        foreach (var (assembly, dbName) in _options.Migrations)
        {
            var db = _engine.GetOrCreateDatabase(dbName);
            _engine.Migrate(assembly, db);
        }

        return Task.CompletedTask;
    }

    public Task StopAsync(CancellationToken cancellationToken) => Task.CompletedTask;
}

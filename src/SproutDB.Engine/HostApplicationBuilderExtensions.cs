using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using SproutDB.Engine.Compilation;
using SproutDB.Engine.Core;
using SproutDB.Engine.Execution;
using SproutDB.Engine.Parsing;

namespace SproutDB.Engine;

public static class HostApplicationBuilderExtensions
{
    public static void AddSproutDB(this HostApplicationBuilder builder)
    {
        builder.Services.AddSingleton<IQueryParser, QueryParser>();
        builder.Services.AddSingleton<IQueryCompiler, QueryCompiler>();
        builder.Services.AddSingleton<IQueryExecutor, SproutDbExecutor>();
        builder.Services.AddSingleton<ISproutDB, SproutDB>();

        builder.Services.AddTransient<IDatabase, Database>();
        builder.Services.AddSingleton<IDataStore, DictionaryDataStore>();

        builder.Services.AddTransient<ISproutConnection, SproutConnection>();
    }
}

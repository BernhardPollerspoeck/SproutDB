using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

namespace SproutDB.Engine.Tests.ISproutConnectionTests;
public abstract class ISproutConnectionTestsSetup : ISproutConnectionTestsCoreData
{
    protected const string META_QUERIES = "Meta Queries";

    protected ISproutConnection _connection = null!;
    protected ISproutDB _server = null!;

    [TestInitialize]
    public void Setup()
    {
        var builder = Host.CreateApplicationBuilder();
        builder.AddSproutDB();
        var app = builder.Build();
        app.Start();
        _connection = app.Services.GetRequiredService<ISproutConnection>();
        _server = app.Services.GetRequiredService<ISproutDB>();
    }

}

using System.Text.Json;
using SproutDB.Core;
using SproutDB.Core.DependencyInjection;
using SproutDB.Core.Server;

var dataDir = Path.Combine(Path.GetTempPath(), "sproutdb-sandbox");

var builder = WebApplication.CreateBuilder(args);
builder.Services.AddSproutDB(options =>
{
    options.DataDirectory = dataDir;
    options.MasterKey = "sdb_ak_1234";
    options.AddMigrations<SproutDB.Sandbox.Migrations.CreateSchema>("garden");
});
builder.Services.AddSproutDBAdmin();

var app = builder.Build();
app.MapSproutDB();
app.MapSproutDBAdmin();

app.Run();


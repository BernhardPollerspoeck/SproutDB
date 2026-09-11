using System.Text.Json;
using SproutDB.Core;
using SproutDB.Core.DependencyInjection;
using SproutDB.Core.Server;

var builder = WebApplication.CreateBuilder(args);

// Optional command line overrides (used by the E2E test client):
//   --data=<dir>          data directory
//   --flushSeconds=<n>    MMF flush / WAL truncate interval (long = WAL replay after a kill)
var dataDir = builder.Configuration["data"] ?? Path.Combine(Path.GetTempPath(), "sproutdb-sandbox");

builder.Services.AddSproutDB(options =>
{
    options.DataDirectory = dataDir;
    options.MasterKey = "sdb_ak_1234";
    options.AddMigrations<SproutDB.Sandbox.Migrations.CreateSchema>("garden");
    if (int.TryParse(builder.Configuration["flushSeconds"], out var flushSeconds))
        options.FlushInterval = TimeSpan.FromSeconds(flushSeconds);
});
builder.Services.AddSproutDBAdmin();
builder.Services.AddSignalR();

var app = builder.Build();
app.MapSproutDB();
app.MapSproutDBHub();
app.MapSproutDBAdmin();

app.Run();

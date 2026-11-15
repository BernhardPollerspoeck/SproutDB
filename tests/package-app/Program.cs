using SproutDB.Core.DependencyInjection;
using SproutDB.Core.Server;

var builder = WebApplication.CreateBuilder(args);

// --data=<dir>: data directory (the automated tests start every app on a fresh one)
var dataDir = builder.Configuration["data"] ?? Path.Combine(Path.GetTempPath(), "sproutdb-package-app");

builder.Services.AddSproutDB(options => options.DataDirectory = dataDir);
builder.Services.AddSproutDBAdmin();

var app = builder.Build();
app.MapSproutDB();
app.MapSproutDBAdmin();

app.Run();

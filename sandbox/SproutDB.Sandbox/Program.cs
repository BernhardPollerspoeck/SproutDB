using System.Text.Json;
using SproutDB.Core;
using SproutDB.Core.DependencyInjection;
using SproutDB.Core.Server;

var dataDir = Path.Combine(Path.GetTempPath(), "sproutdb-sandbox");

var builder = WebApplication.CreateBuilder(args);
builder.Services.AddSproutDB(options =>
{
    options.DataDirectory = dataDir;
});
builder.Services.AddSproutDBAdmin();

var app = builder.Build();
app.UseStaticFiles();
app.UseAntiforgery();
app.MapSproutDB();
app.MapSproutDBAdmin();

var engine = app.Services.GetRequiredService<SproutEngine>();
var server = app.Services.GetRequiredService<ISproutServer>();

var json = new JsonSerializerOptions { PropertyNamingPolicy = JsonNamingPolicy.SnakeCaseLower, WriteIndented = true };
void Run(string query, string db = "testdb")
{
    Console.WriteLine($"\n> {query}");
    Console.WriteLine(JsonSerializer.Serialize(engine.Execute(query, db), json));
}

// -- Setup via ISproutServer --
Console.WriteLine("=== DI SETUP ===");
Console.WriteLine($"Engine resolved: {engine is not null}");
Console.WriteLine($"ISproutServer same instance: {ReferenceEquals(engine, server)}");

var db = server.GetOrCreateDatabase("testdb");
Console.WriteLine($"Database: {db.Name}");

// -- Schema via Fluent API (idempotent) --
Console.WriteLine("\n=== FLUENT API ===");
var describe = engine.Execute("describe users", "testdb");
if (describe.Operation == SproutOperation.Error)
{
    db.CreateTable("users")
        .AddColumn<string>("name", 100)
        .AddColumn<string>("email", 320, strict: true)
        .AddColumn<byte>("age")
        .AddColumn<bool>("active", defaultValue: "true")
        .AddColumn<int>("score")
        .Execute();
    Console.WriteLine("Table 'users' created via Fluent API");

    Console.WriteLine("\n=== INSERTS ===");
    Run("upsert users {name: 'Alice', email: 'alice@test.com', age: 28, score: 150}");
    Run("upsert users {name: 'Bob', email: 'bob@test.com', age: 35, score: -20}");
    Run("upsert users {name: 'Charlie', email: 'charlie@test.com', age: 22}");

    db.AddColumn<bool>("users", "premium", defaultValue: "false");
    Console.WriteLine("Column 'premium' added");
    db.AlterColumn("users", "name", 500);
    Console.WriteLine("Column 'name' expanded to 500");
}
else
{
    Console.WriteLine("Table 'users' already exists — skipping setup");
}

Console.WriteLine("\n=== GET ===");
Run("get users");

// -- HTTP endpoint info --
Console.WriteLine("\n=== HTTP ENDPOINT ===");
Console.WriteLine("POST /sproutdb/query  (Header: X-SproutDB-Database)");
Console.WriteLine("Starting web server...");

app.Run();

// -- Cleanup --
Console.WriteLine($"\nData directory: {dataDir}");
engine.Dispose();
Console.WriteLine("Done.");

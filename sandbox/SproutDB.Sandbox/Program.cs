using System.Text.Json;
using SproutDB.Core;

var dataDir = Path.Combine($"sproutdb-sandbox-{Guid.NewGuid()}");
using var engine = new SproutEngine(dataDir);

var json = new JsonSerializerOptions { PropertyNamingPolicy = JsonNamingPolicy.SnakeCaseLower, WriteIndented = true };
void Run(string query, string db = "testdb")
{
    Console.WriteLine($"Executing query: {query} (database: {db})");
    Console.WriteLine(JsonSerializer.Serialize(engine.Execute(query, db), json));
}

// -- Spielwiese --

Run("create database");
Run("create table users (name string 100, email string 320 strict, age ubyte, active bool default true, score sint)");

Console.WriteLine("\n=== INSERTS ===\n");
Run("upsert users {name: 'John', email: 'john@test.com', age: 25, score: 100}");
Run("upsert users {name: 'Jane', email: 'jane@test.com', age: 30}");
Run("upsert users {}"); // only defaults

Console.WriteLine("\n=== UPDATE ===\n");
Run("upsert users {id: 1, name: 'John Doe', score: -50}");

Console.WriteLine("\n=== FILES ON DISK ===\n");
var tablePath = Path.Combine(dataDir, "testdb", "users");
foreach (var file in Directory.GetFiles(tablePath))
{
    var info = new FileInfo(file);
    Console.WriteLine($"  {info.Name,-20} {info.Length,10:N0} bytes");
}

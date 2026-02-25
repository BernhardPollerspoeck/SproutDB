using System.Text.Json;
using SproutDB.Core;

var dataDir = Path.Combine($"sproutdb-sandbox-{Guid.NewGuid()}");
using var engine = new SproutEngine(dataDir);

var json = new JsonSerializerOptions { PropertyNamingPolicy = JsonNamingPolicy.SnakeCaseLower, WriteIndented = true };
void Run(string query, string db = "testdb")
{
    Console.WriteLine($"\n> {query}");
    Console.WriteLine(JsonSerializer.Serialize(engine.Execute(query, db), json));
}

// -- Setup --

Run("create database");
Run("create table users (name string 100, email string 320 strict, age ubyte, active bool default true, score sint)");

Console.WriteLine("\n=== INSERTS ===");
Run("upsert users {name: 'Alice', email: 'alice@test.com', age: 28, score: 150}");
Run("upsert users {name: 'Bob', email: 'bob@test.com', age: 35, score: -20}");
Run("upsert users {name: 'Charlie', email: 'charlie@test.com', age: 22}");
Run("upsert users {}"); // only defaults

Console.WriteLine("\n=== UPDATE ===");
Run("upsert users {id: 2, name: 'Bob Updated', score: 100}");

Console.WriteLine("\n=== GET: all rows, all columns ===");
Run("get users");

Console.WriteLine("\n=== GET: select specific columns ===");
Run("get users select name, email");

Console.WriteLine("\n=== GET: select only id and name ===");
Run("get users select id, name");

Console.WriteLine("\n=== GET: select single column ===");
Run("get users select score");

Console.WriteLine("\n=== GET: error – unknown column ===");
Run("get users select nonexistent");

Console.WriteLine("\n=== FILES ON DISK ===");
var tablePath = Path.Combine(dataDir, "testdb", "users");
foreach (var file in Directory.GetFiles(tablePath))
{
    var info = new FileInfo(file);
    Console.WriteLine($"  {info.Name,-20} {info.Length,10:N0} bytes");
}

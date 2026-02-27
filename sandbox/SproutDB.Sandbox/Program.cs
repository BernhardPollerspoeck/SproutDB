using System.Text.Json;
using SproutDB.Core;

var dataDir = Path.Combine($"sproutdb-sandbox-{Guid.NewGuid()}");
var engine = new SproutEngine(dataDir);

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

// Show WAL file
var walPath = Path.Combine(dataDir, "testdb", "_wal");
if (File.Exists(walPath))
{
    var walInfo = new FileInfo(walPath);
    Console.WriteLine($"\n  _wal                 {walInfo.Length,10:N0} bytes");
}

// -- WAL Replay Demo --
Console.WriteLine("\n=== WAL REPLAY DEMO ===");
Console.WriteLine("Disposing engine (simulating shutdown)...");
engine.Dispose();

Console.WriteLine("Opening new engine on same data directory...");
using var engine2 = new SproutEngine(dataDir);

var json2 = new JsonSerializerOptions { PropertyNamingPolicy = JsonNamingPolicy.SnakeCaseLower, WriteIndented = true };
void Run2(string query, string db = "testdb")
{
    Console.WriteLine($"\n> {query}");
    Console.WriteLine(JsonSerializer.Serialize(engine2.Execute(query, db), json2));
}

Console.WriteLine("Data should be recovered from WAL:");
Run2("get users select id, name, score");

Console.WriteLine("\nInsert after restart (should get id=5):");
Run2("upsert users {name: 'Diana', score: 999}");

// -- PURGE Demo --
Console.WriteLine("\n=== PURGE COLUMN ===");
Run2("purge column users.score");

Console.WriteLine("\n=== GET after purge column (score gone) ===");
Run2("get users");

Console.WriteLine("\n=== PURGE TABLE ===");
Run2("purge table users");

Console.WriteLine("\n=== GET after purge table (table gone) ===");
Run2("get users");

Console.WriteLine("\n=== PURGE DATABASE ===");
Run2("purge database");

Console.WriteLine("\n=== GET after purge database (db gone) ===");
Run2("get users");

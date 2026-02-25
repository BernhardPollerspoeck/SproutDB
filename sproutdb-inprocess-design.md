# SproutDB In-Process Interface Design

## Grundprinzipien
- Zwei Zugriffswege: Query-String (A) und Typed LINQ (B)
- Query-String ist der Core – HTTP, WAL Replay und In-Process nutzen denselben Parser
- LINQ ist ein dünner Layer drüber für .NET-native DX
- Migrations pro Database via `IMigration` Interface
- Jede Database trackt ihre Migrations selbst in `_migrations` Table

---

## Server Setup

### ASP.NET (Server + HTTP + SignalR)
```csharp
var builder = WebApplication.CreateBuilder(args);
builder.Services.AddSproutDB(options =>
{
    options.DataDirectory = "/data/sproutdb";
});

var app = builder.Build();

var sprout = app.Services.GetRequiredService<ISproutServer>();

// Databases erstellen (stilles OK wenn schon vorhanden)
var shop = sprout.GetOrCreateDatabase("shop");
var analytics = sprout.GetOrCreateDatabase("analytics");

// Migrations pro Database
sprout.Migrate(typeof(ShopMigrations).Assembly, shop);
sprout.Migrate(typeof(AnalyticsMigrations).Assembly, analytics);

// Alle Tenant-DBs migrieren
foreach (var db in sprout.GetDatabases())
{
    if (db.Name.StartsWith("tenant_"))
        sprout.Migrate(typeof(TenantMigrations).Assembly, db);
}

app.MapSproutDB();       // POST /query
app.MapSproutDBHub();    // SignalR real-time changes
app.Run();
```

### Embedded (Generic Host, kein HTTP)
```csharp
var builder = Host.CreateDefaultBuilder(args);
builder.ConfigureServices(services =>
{
    services.AddSproutDB(options =>
    {
        options.DataDirectory = "/data/sproutdb";
    });
});

var host = builder.Build();

var sprout = host.Services.GetRequiredService<ISproutServer>();
var shop = sprout.GetOrCreateDatabase("shop");
sprout.Migrate(typeof(ShopMigrations).Assembly, shop);
```

### Runtime: Neuen Tenant erstellen
```csharp
var tenantDb = sprout.GetOrCreateDatabase($"tenant_{tenantId}");
sprout.Migrate(typeof(TenantMigrations).Assembly, tenantDb);
```

---

## Database Zugriff

```csharp
var db = sprout.SelectDatabase("shop");
var allDbs = sprout.GetDatabases();  // Liste aller Databases mit Name
```

---

## Query-String API (Weg A)

```csharp
// Get
var result = db.Query("get users where age > 18");

// Upsert
var result = db.Query("upsert users {name: \"John\", age: 25}");

// Delete
var result = db.Query("delete users where active = false");

// Schema
db.Query("add column users.premium bool");
db.Query("describe users");
```

> Selber Parser wie HTTP und WAL Replay. Ein Codepath für alles.
> Result ist immer dasselbe JSON-kompatible Objekt wie bei HTTP.

---

## Typed LINQ API (Weg B)

### User-Klasse
```csharp
public class User
{
    public ulong Id { get; set; }
    public string Name { get; set; }
    public string Email { get; set; }
    public byte Age { get; set; }
    public bool Active { get; set; }
}
```

### Queries
```csharp
var users = db.Table<User>("users");

// Get — returns SproutResponse (same object as HTTP and Query String)
var response = users.Where(u => u.Age > 18).Run();

// Get — typed results
var adults = users.Where(u => u.Age > 18).ToList();
var names = users.Where(u => u.Active).Select(u => u.Name).ToList();
var john = users.FirstOrDefault(u => u.Id == 42);

// Count
var count = users.Where(u => u.Active).Count();

// Order + Limit
var top10 = users.Where(u => u.Active)
    .OrderByDescending(u => u.Age)
    .Take(10)
    .Run();

// Upsert — typed object (all fields)
users.Upsert(new User { Name = "John", Email = "john@test.com", Age = 25 });

// Upsert — partial update mit anonymem Objekt (nur age ändert sich)
users.Upsert(new { Id = 1ul, Age = (byte)26 });

// Upsert on match column
users.Upsert(new User { Email = "john@test.com", Name = "John Doe" }, on: u => u.Email);

// Bulk Upsert
users.Upsert(new[] { user1, user2, user3 }, on: u => u.Email);

// Delete
users.Delete(u => u.Active == false);
users.Delete(u => u.Id == 42);
```

> Expression Trees werden intern in Query-Objekte übersetzt.
> Kein SQL-String-Gebastel, volle IntelliSense, compile-safe.

---

## Migrations

### Interface
```csharp
public interface IMigration
{
    int Order { get; }
    void Up(ISproutDatabase db);
}
```

> Migration weiß nicht gegen welche Database sie läuft – das bestimmt der Aufrufer.
> Selbe Migration-Assembly kann gegen mehrere Databases laufen (z.B. Multi-Tenant).

### Fluent API für Schema
```csharp
public class CreateUsersTable : IMigration
{
    public int Order => 1;

    public void Up(ISproutDatabase db)
    {
        db.CreateTable("users")
            .AddColumn<string>("name")
            .AddColumn<string>("email", size: 320, strict: true)
            .AddColumn<int>("age")
            .AddColumn<bool>("active", defaultValue: true)
            .AddColumn<string>("bio", size: 5000);
    }
}

public class AddPremiumColumn : IMigration
{
    public int Order => 2;

    public void Up(ISproutDatabase db)
    {
        db.AddColumn<bool>("users", "premium", defaultValue: false);
    }
}

public class ResizeBio : IMigration
{
    public int Order => 3;

    public void Up(ISproutDatabase db)
    {
        db.AlterColumn("users", "bio", size: 10000);
    }
}
```

### Migration Tracking
Jede Database hat eine `_migrations` Table (read-only für User):
```json
{id: 1, name: "CreateUsersTable", order: 1, executed: "2026-02-25 10:30:00.0000"}
{id: 2, name: "AddPremiumColumn", order: 2, executed: "2026-02-25 10:30:00.0001"}
{id: 3, name: "ResizeBio", order: 3, executed: "2026-02-25 10:30:00.0002"}
```

### Startup-Ablauf
1. Server startet, alle registrierten Assemblies werden gescannt
2. Für jede Database: `_migrations` Table lesen
3. Fehlende Migrations in Order ausführen
4. Neue Einträge in `_migrations` schreiben
5. Ready

> Migrations laufen VOR dem Öffnen der HTTP/SignalR Endpoints.
> Fehlgeschlagene Migration → Server startet nicht. Kein halber Zustand.

---

## Database Export/Import

### Export
```csharp
db.ExportToZip("/backups/shop-2026-02-25.zip");
```
> Database = Verzeichnis. ZIP = vollständiges Backup inkl. Daten, Schema, Migrations-History.

### Import
```csharp
sprout.ImportFromZip("/backups/shop-2026-02-25.zip", "shop_restored");
```
> Unzip in neues Verzeichnis, MMFs öffnen, fertig. Kein Restore-Prozess.

---

## Change Notifications (In-Process)

```csharp
var users = db.Table<User>("users");

// Subscribe
users.OnChange(change =>
{
    // change.Data = List<User>
    // change.Operation = "insert" | "update" | "delete"
    // change.Affected = int
});
```

> Selbes Event wie SignalR, nur direkt als Callback statt über Netzwerk.
> Intern: selber Mechanismus, nur ohne Serialisierung/Deserialisierung.

# SproutDB C# Integration Reference

## Service Registration

```csharp
using SproutDB.Core.DependencyInjection;

// Option A: Builder callback
services.AddSproutDB(options =>
{
    options.DataDirectory = "/data/sproutdb";
    options.DefaultPageSize = 100;
    options.BulkLimit = 100;
    options.AddMigrations<MyMigrations>("database_name");
});

// Option B: from IConfiguration ("SproutDB" section)
services.AddSproutDB(configuration);

// Option C: Configuration + code override
services.AddSproutDB(configuration, builder =>
{
    builder.DataDirectory = "/override/path";
});
```

## Auth (opt-in)

```csharp
services.AddSproutDBAuth(o => o.MasterKey = "sdb_ak_...");
```

## HTTP + Admin Endpoints

```csharp
var app = builder.Build();
app.MapSproutDB();        // POST /sproutdb/query
app.MapSproutDBHub();     // SignalR /sproutdb/changes
app.MapSproutDBAdmin();   // Blazor Admin UI /sproutdb/admin
```

## Direct Engine Access (without DI)

```csharp
var engine = new SproutEngine(new SproutEngineSettings
{
    DataDirectory = "/data/sproutdb"
});
var db = engine.GetOrCreateDatabase("mydb");
var result = db.Query("get users");
engine.Dispose();
```

---

## Interfaces

```csharp
// ISproutServer (Singleton)
ISproutDatabase GetOrCreateDatabase(string name);
ISproutDatabase SelectDatabase(string name);
IReadOnlyList<ISproutDatabase> GetDatabases();
void Migrate(Assembly assembly, ISproutDatabase database);

// ISproutDatabase
string Name { get; }
SproutResponse Query(string query);
IDisposable OnChange(string table, Action<SproutResponse> callback);
```

## SproutResponse

```csharp
public sealed class SproutResponse
{
    public SproutOperation Operation { get; init; }
    public List<Dictionary<string, object?>>? Data { get; init; }
    public int Affected { get; init; }
    public SchemaInfo? Schema { get; init; }
    public PagingInfo? Paging { get; init; }
    public List<SproutError>? Errors { get; init; }
    public string? AnnotatedQuery { get; init; }
}
```

---

## Migrations

### Interface

```csharp
public interface IMigration
{
    int Order { get; }
    MigrationMode Mode => MigrationMode.Once;
    void Up(ISproutDatabase db);
}
```

### Modes

| Mode | Behavior |
|------|----------|
| `Once` | Runs once, tracked in `_migrations` table, skipped on restart |
| `OnStartup` | Runs every start, NOT tracked. For cleanup tasks |

### Example

```csharp
public sealed class CreateUsers : IMigration
{
    public int Order => 1;
    public void Up(ISproutDatabase db)
    {
        db.Query("create table users (name string 100, email string 320 strict, active bool default true)");
        db.Query("create unique index users.email");
    }
}

public sealed class AddAge : IMigration
{
    public int Order => 2;
    public void Up(ISproutDatabase db)
    {
        db.Query("add column users.age ubyte");
    }
}

public sealed class CleanupOnStart : IMigration
{
    public int Order => 99;
    public MigrationMode Mode => MigrationMode.OnStartup;
    public void Up(ISproutDatabase db)
    {
        db.Query("delete sessions where active = false");
    }
}
```

### Registration

```csharp
services.AddSproutDB(options =>
{
    options.DataDirectory = "/data";
    options.AddMigrations<MyMigrations.Marker>("shop");
});
```

`SproutMigrationHostedService` runs migrations BEFORE Kestrel starts. On failure → server won't start.

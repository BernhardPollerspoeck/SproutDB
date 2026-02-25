# SproutDB API Reference

## HTTP API

### Endpoint
```
POST /query
Content-Type: text/plain
```

### Request Headers
| Header | Pflicht | Beschreibung |
|---|---|---|
| `X-SproutDB-Database` | Ja | Aktive Datenbank. Immer Pflicht – auch bei `create database` und `purge database`. |
| `X-SproutDB-ApiKey` | Ja (wenn Auth aktiv) | API Key für Authentifizierung |

---

## Response Format

Jede Response hat exakt dasselbe Format. Alle Felder sind immer vorhanden – nicht benötigte Felder sind `null` bzw. `0`.

```json
{
  "operation": 1,
  "data": [],
  "affected": 0,
  "schema": null,
  "paging": null,
  "errors": null,
  "annotated_query": null
}
```

### Felder
| Feld | Typ | Beschreibung |
|---|---|---|
| `operation` | byte (enum) | Operation die ausgeführt wurde |
| `data` | array | Records bei get/upsert/delete, null bei Schema-Ops |
| `affected` | int | Anzahl betroffener Records, 0 bei Schema-Ops |
| `schema` | object | Table-Schema bei describe und Schema-Ops, null bei Daten-Ops |
| `paging` | object | Paging-Info bei gepagten Results, null sonst |
| `errors` | array | Error-Details bei Fehler, null bei Erfolg |
| `annotated_query` | string | Query mit `##`-Kommentaren an Fehlerstellen, null bei Erfolg |

### Operation Enum
| Byte | Operation |
|---|---|
| 0 | error |
| 1 | get |
| 2 | upsert |
| 3 | delete |
| 4 | describe |
| 5 | create_table |
| 6 | create_database |
| 7 | purge_table |
| 8 | purge_database |
| 9 | purge_column |
| 10 | add_column |
| 11 | rename_column |
| 12 | alter_column |
| 13 | create_index |
| 14 | purge_index |

---

## HTTP Status Codes

| Status | Wann | Error Codes |
|---|---|---|
| 200 | Alles OK | – |
| 400 | Query-Fehler | SYNTAX_ERROR, TYPE_MISMATCH, NOT_NULLABLE, TYPE_NARROWING, STRICT_VIOLATION, BULK_LIMIT |
| 401 | Auth fehlend/ungültig | AUTH_REQUIRED, AUTH_INVALID |
| 403 | Keine Berechtigung | PERMISSION_DENIED |
| 404 | Ressource nicht gefunden | UNKNOWN_TABLE, UNKNOWN_COLUMN, UNKNOWN_DATABASE |
| 409 | Ressource existiert bereits | TABLE_EXISTS, DATABASE_EXISTS |

---

## Beispiele pro Operation

### get
```
POST /query
X-SproutDB-Database: shop

get users where age > 18
```
```json
{
  "operation": 1,
  "data": [
    {"id": 1, "name": "John", "email": "john@test.com", "age": 25, "active": true},
    {"id": 2, "name": "Jane", "email": "jane@test.com", "age": 30, "active": true}
  ],
  "affected": 2,
  "schema": null,
  "paging": null,
  "errors": null,
  "annotated_query": null
}
```

### get mit select
```
get users select name, age where age > 18
```
```json
{
  "operation": 1,
  "data": [
    {"name": "John", "age": 25},
    {"name": "Jane", "age": 30}
  ],
  "affected": 2,
  "schema": null,
  "paging": null,
  "errors": null,
  "annotated_query": null
}
```

### get mit -select (Exclude)
```
get users -select email, active where age > 18
```
```json
{
  "operation": 1,
  "data": [
    {"id": 1, "name": "John", "age": 25},
    {"id": 2, "name": "Jane", "age": 30}
  ],
  "affected": 2,
  "schema": null,
  "paging": null,
  "errors": null,
  "annotated_query": null
}
```

### get mit Paging
```
get users where active = true
```
```json
{
  "operation": 1,
  "data": [
    {"id": 1, "name": "John", "email": "john@test.com", "age": 25, "active": true}
  ],
  "affected": 100,
  "schema": null,
  "paging": {
    "total": 1523,
    "page_size": 100,
    "page": 1,
    "next": "get users where active = true page 2 size 100"
  },
  "errors": null,
  "annotated_query": null
}
```

### upsert (Insert)
```
upsert users {name: 'John', email: 'john@test.com', age: 25}
```
```json
{
  "operation": 2,
  "data": [
    {"id": 1, "name": "John", "email": "john@test.com", "age": 25, "active": true}
  ],
  "affected": 1,
  "schema": null,
  "paging": null,
  "errors": null,
  "annotated_query": null
}
```

### upsert (Update)
```
upsert users {id: 1, name: 'John Doe'}
```
```json
{
  "operation": 2,
  "data": [
    {"id": 1, "name": "John Doe", "email": "john@test.com", "age": 25, "active": true}
  ],
  "affected": 1,
  "schema": null,
  "paging": null,
  "errors": null,
  "annotated_query": null
}
```

### upsert (Bulk)
```
upsert users [
  {name: 'John', email: 'john@test.com'},
  {name: 'Jane', email: 'jane@test.com'}
] on email
```
```json
{
  "operation": 2,
  "data": [
    {"id": 1, "name": "John", "email": "john@test.com", "age": 25, "active": true},
    {"id": 3, "name": "Jane", "email": "jane@test.com", "age": null, "active": true}
  ],
  "affected": 2,
  "schema": null,
  "paging": null,
  "errors": null,
  "annotated_query": null
}
```

### delete
```
delete users where active = false
```
```json
{
  "operation": 3,
  "data": [
    {"id": 5, "name": "Bob", "email": "bob@test.com", "age": 40, "active": false}
  ],
  "affected": 1,
  "schema": null,
  "paging": null,
  "errors": null,
  "annotated_query": null
}
```

### describe (Table)
```
describe users
```
```json
{
  "operation": 4,
  "data": null,
  "affected": 0,
  "schema": {
    "table": "users",
    "columns": [
      {"name": "id", "type": "ulong", "nullable": false, "default": null, "strict": true, "auto": true},
      {"name": "name", "type": "string", "size": 255, "nullable": true, "default": null, "strict": false},
      {"name": "email", "type": "string", "size": 320, "nullable": true, "default": null, "strict": true},
      {"name": "age", "type": "ubyte", "nullable": true, "default": null, "strict": false},
      {"name": "active", "type": "bool", "nullable": false, "default": true, "strict": false}
    ]
  },
  "paging": null,
  "errors": null,
  "annotated_query": null
}
```

### describe (alle Tabellen)
```
describe
```
```json
{
  "operation": 4,
  "data": null,
  "affected": 0,
  "schema": {
    "tables": ["users", "orders", "products"]
  },
  "paging": null,
  "errors": null,
  "annotated_query": null
}
```

### create database
```
X-SproutDB-Database: shop

create database
```
```json
{
  "operation": 6,
  "data": null,
  "affected": 0,
  "schema": {
    "database": "shop"
  },
  "paging": null,
  "errors": null,
  "annotated_query": null
}
```

### create table
```
create table users (name string, email string 320 strict, age ubyte, active bool default true)
```
```json
{
  "operation": 5,
  "data": null,
  "affected": 0,
  "schema": {
    "table": "users",
    "columns": [
      {"name": "id", "type": "ulong", "nullable": false, "default": null, "strict": true, "auto": true},
      {"name": "name", "type": "string", "size": 255, "nullable": true, "default": null, "strict": false},
      {"name": "email", "type": "string", "size": 320, "nullable": true, "default": null, "strict": true},
      {"name": "age", "type": "ubyte", "nullable": true, "default": null, "strict": false},
      {"name": "active", "type": "bool", "nullable": false, "default": true, "strict": false}
    ]
  },
  "paging": null,
  "errors": null,
  "annotated_query": null
}
```

### add column
```
add column users.premium bool
```
```json
{
  "operation": 10,
  "data": null,
  "affected": 0,
  "schema": {
    "table": "users",
    "columns": [
      {"name": "id", "type": "ulong", "nullable": false, "default": null, "strict": true, "auto": true},
      {"name": "name", "type": "string", "size": 255, "nullable": true, "default": null, "strict": false},
      {"name": "email", "type": "string", "size": 320, "nullable": true, "default": null, "strict": true},
      {"name": "age", "type": "ubyte", "nullable": true, "default": null, "strict": false},
      {"name": "active", "type": "bool", "nullable": false, "default": true, "strict": false},
      {"name": "premium", "type": "bool", "nullable": true, "default": null, "strict": false}
    ]
  },
  "paging": null,
  "errors": null,
  "annotated_query": null
}
```

### rename column
```
rename column users.premium to is_premium
```
```json
{
  "operation": 11,
  "data": null,
  "affected": 0,
  "schema": {
    "table": "users",
    "columns": [
      {"name": "id", "type": "ulong", "nullable": false, "default": null, "strict": true, "auto": true},
      {"name": "name", "type": "string", "size": 255, "nullable": true, "default": null, "strict": false},
      {"name": "email", "type": "string", "size": 320, "nullable": true, "default": null, "strict": true},
      {"name": "age", "type": "ubyte", "nullable": true, "default": null, "strict": false},
      {"name": "active", "type": "bool", "nullable": false, "default": true, "strict": false},
      {"name": "is_premium", "type": "bool", "nullable": true, "default": null, "strict": false}
    ]
  },
  "paging": null,
  "errors": null,
  "annotated_query": null
}
```

### alter column
```
alter column users.name string 500
```
```json
{
  "operation": 12,
  "data": null,
  "affected": 0,
  "schema": {
    "table": "users",
    "columns": [
      {"name": "id", "type": "ulong", "nullable": false, "default": null, "strict": true, "auto": true},
      {"name": "name", "type": "string", "size": 500, "nullable": true, "default": null, "strict": false},
      {"name": "email", "type": "string", "size": 320, "nullable": true, "default": null, "strict": true},
      {"name": "age", "type": "ubyte", "nullable": true, "default": null, "strict": false},
      {"name": "active", "type": "bool", "nullable": false, "default": true, "strict": false},
      {"name": "is_premium", "type": "bool", "nullable": true, "default": null, "strict": false}
    ]
  },
  "paging": null,
  "errors": null,
  "annotated_query": null
}
```

### purge column
```
purge column users.is_premium
```
```json
{
  "operation": 9,
  "data": null,
  "affected": 0,
  "schema": {
    "table": "users",
    "columns": [
      {"name": "id", "type": "ulong", "nullable": false, "default": null, "strict": true, "auto": true},
      {"name": "name", "type": "string", "size": 500, "nullable": true, "default": null, "strict": false},
      {"name": "email", "type": "string", "size": 320, "nullable": true, "default": null, "strict": true},
      {"name": "age", "type": "ubyte", "nullable": true, "default": null, "strict": false},
      {"name": "active", "type": "bool", "nullable": false, "default": true, "strict": false}
    ]
  },
  "paging": null,
  "errors": null,
  "annotated_query": null
}
```

### purge table
```
purge table users
```
```json
{
  "operation": 7,
  "data": null,
  "affected": 0,
  "schema": {
    "table": "users"
  },
  "paging": null,
  "errors": null,
  "annotated_query": null
}
```

### purge database
```
X-SproutDB-Database: shop

purge database
```
```json
{
  "operation": 8,
  "data": null,
  "affected": 0,
  "schema": {
    "database": "shop"
  },
  "paging": null,
  "errors": null,
  "annotated_query": null
}
```

### create index
```
create index users.email
```
```json
{
  "operation": 13,
  "data": null,
  "affected": 0,
  "schema": {
    "table": "users",
    "column": "email",
    "index": "users.email"
  },
  "paging": null,
  "errors": null,
  "annotated_query": null
}
```

### purge index
```
purge index users.email
```
```json
{
  "operation": 14,
  "data": null,
  "affected": 0,
  "schema": {
    "table": "users",
    "column": "email",
    "index": "users.email"
  },
  "paging": null,
  "errors": null,
  "annotated_query": null
}
```

### Error Response (400 – Query-Fehler)
```
get userss where agee > 'eighteen'
```
```json
{
  "operation": 0,
  "data": null,
  "affected": 0,
  "schema": null,
  "paging": null,
  "errors": [
    {"code": "UNKNOWN_TABLE", "message": "Table 'userss' does not exist"},
    {"code": "UNKNOWN_COLUMN", "message": "Column 'agee' does not exist in table 'users'"},
    {"code": "TYPE_MISMATCH", "message": "Expected int, got string for column 'age'"}
  ],
  "annotated_query": "get userss ##unknown table 'userss'## where agee ##unknown column 'agee'## > 'eighteen' ##type mismatch: expected ubyte, got string##"
}
```

### Error Response (404 – Not Found)
```
describe users
```
```json
{
  "operation": 0,
  "data": null,
  "affected": 0,
  "schema": null,
  "paging": null,
  "errors": [
    {"code": "UNKNOWN_TABLE", "message": "Table 'users' does not exist"}
  ],
  "annotated_query": "describe users ##unknown table 'users'##"
}
```

### Error Response (409 – Conflict)
```
create table users (name string)
```
```json
{
  "operation": 0,
  "data": null,
  "affected": 0,
  "schema": null,
  "paging": null,
  "errors": [
    {"code": "TABLE_EXISTS", "message": "Table 'users' already exists"}
  ],
  "annotated_query": "create table users ##table 'users' already exists## (name string)"
}
```

### Error Response (401 – Unauthorized)
```json
{
  "operation": 0,
  "data": null,
  "affected": 0,
  "schema": null,
  "paging": null,
  "errors": [
    {"code": "AUTH_REQUIRED", "message": "API key required"}
  ],
  "annotated_query": null
}
```

### Error Response (403 – Forbidden)
```json
{
  "operation": 0,
  "data": null,
  "affected": 0,
  "schema": null,
  "paging": null,
  "errors": [
    {"code": "PERMISSION_DENIED", "message": "Role 'reader' cannot execute upsert"}
  ],
  "annotated_query": null
}
```

---

## SignalR

### Konzept
SignalR Events nutzen exakt dasselbe Response-Format wie HTTP. Kein separates Event-Modell.

### Hub
```
Endpoint: /sproutdb/changes
Auth: X-SproutDB-ApiKey via Query String oder Header
```

### Subscribing

**Client → Server:**
| Methode | Beschreibung |
|---|---|
| `Subscribe(string database, string table)` | Tritt Group bei, empfängt Data-Changes |
| `Unsubscribe(string database, string table)` | Verlässt Group |

**Server → Client:**
| Methode | Beschreibung |
|---|---|
| `OnChange(SproutResponse event)` | Selbes Response-Objekt wie HTTP |

### Gruppen
- Data-Changes: `{database}.{table}` (z.B. `shop.users`)
- Schema-Changes: `{database}._schema` (z.B. `shop._schema`)

### Beispiel: Subscribe auf Data-Changes
```csharp
var connection = new HubConnectionBuilder()
    .WithUrl("https://localhost:5001/sproutdb/changes", options =>
    {
        options.Headers.Add("X-SproutDB-ApiKey", "sdb_ak_xxxxx");
    })
    .Build();

connection.On<SproutResponse>("OnChange", response =>
{
    // response.Operation, response.Data, response.Affected etc.
});

await connection.StartAsync();
await connection.InvokeAsync("Subscribe", "shop", "users");
```

### Beispiel: Subscribe auf Schema-Changes
```csharp
await connection.InvokeAsync("Subscribe", "shop", "_schema");
```

### Verhalten
- Kein Listener in der Group → kein Overhead
- Bei Disconnect räumt SignalR die Group-Membership automatisch auf
- Permissions greifen: nur Databases für die der API Key Zugriff hat
- Wer die Änderung via HTTP macht sieht die Response dort – alle anderen via SignalR

---

## Server Setup (ASP.NET)

### Minimal Setup
```csharp
var builder = WebApplication.CreateBuilder(args);

builder.Services.AddSproutDB(options =>
{
    options.DataDirectory = "/data/sproutdb";
});

var app = builder.Build();

app.MapSproutDB();          // POST /query
app.MapSproutDBHub();       // /sproutdb/changes

app.Run();
```

### Mit Auth und Settings
```csharp
builder.Services.AddSproutDB(options =>
{
    options.DataDirectory = "/data/sproutdb";
    options.DefaultPageSize = 100;
    options.WalFlushIntervalSeconds = 5;
    options.PreAllocateChunkSize = 10000;
    options.AutoIndex = new AutoIndexOptions
    {
        Enabled = true,
        UsageThresholdPercent = 30,
        SelectivityThresholdPercent = 95,
        ReadWriteRatioThreshold = 3.0,
        UnusedIndexRemovalDays = 30
    };
});

builder.Services.AddSproutDBAuth(options =>
{
    options.RequireApiKey = true;
});

var app = builder.Build();

app.MapSproutDB();
app.MapSproutDBHub();

app.Run();
```

### Mit Migrations
```csharp
var builder = WebApplication.CreateBuilder(args);

builder.Services.AddSproutDB(options =>
{
    options.DataDirectory = "/data/sproutdb";
});

var app = builder.Build();

var sprout = app.Services.GetRequiredService<ISproutServer>();

// Database erstellen und migrieren
var shop = sprout.GetOrCreateDatabase("shop");
sprout.Migrate(typeof(ShopMigrations).Assembly, shop);

// Multi-Tenant
foreach (var tenant in GetTenants())
{
    var db = sprout.GetOrCreateDatabase($"tenant_{tenant.Id}");
    sprout.Migrate(typeof(TenantMigrations).Assembly, db);
}

app.MapSproutDB();
app.MapSproutDBHub();

app.Run();
```

### appsettings.json
```json
{
  "SproutDB": {
    "DataDirectory": "/data/sproutdb",
    "DefaultPageSize": 100,
    "WalFlushIntervalSeconds": 5,
    "PreAllocateChunkSize": 10000,
    "AutoIndex": {
      "Enabled": true,
      "UsageThresholdPercent": 30,
      "SelectivityThresholdPercent": 95,
      "ReadWriteRatioThreshold": 3.0,
      "UnusedIndexRemovalDays": 30
    },
    "Auth": {
      "RequireApiKey": true
    }
  }
}
```

### Docker Compose
```yaml
services:
  sproutdb:
    image: sproutdb:latest
    ports:
      - "5100:8080"
    volumes:
      - sproutdb-data:/data/sproutdb
    environment:
      - SproutDB__DefaultPageSize=100
      - SproutDB__Auth__RequireApiKey=true

volumes:
  sproutdb-data:
```

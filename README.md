# SproutDB 🌱

**A .NET-native database engine. Embedded or networked. No Docker, no config, no ops.**

SproutDB is a database that ships as a NuGet package. You add it to your ASP.NET project, wire it up in two lines, and you have a fully functional database server with HTTP and real-time SignalR support. No external process, no connection strings pointing to some container, no database admin needed.

It works just as well embedded — reference the core library, call your queries in-process, skip the network entirely. Same engine, same query language, same behavior. You choose how you want to talk to it.

The query language is designed to be readable. No `SELECT * FROM` boilerplate. You write `get users where active = true` and get JSON back. Every response has the same shape, every error tells you exactly where you went wrong. It's built for developers who want to move fast and not fight their database.

SproutDB is schema-typed with a full set of numeric types so you control exactly how much storage each column uses. It handles migrations, supports multi-tenant setups out of the box, and notifies your app in real-time when data changes. If you've ever wished your database was just another library in your project — that's what this is.

---

> **Why?** Because there is no .NET-native database that you can add as a NuGet package and that works both embedded and networked at the same time. Everything else forces a compromise that shouldn't be necessary.

---

## Quick Start

### As a Server (ASP.NET)

```csharp
var builder = WebApplication.CreateBuilder(args);
builder.Services.AddSproutDB(options =>
{
    options.DataDirectory = "/data/sproutdb";
});
builder.Services.AddSignalR();   // needed for MapSproutDBHub()

var app = builder.Build();

var sprout = app.Services.GetRequiredService<ISproutServer>();
var shop = sprout.GetOrCreateDatabase("shop");
sprout.Migrate(typeof(ShopMigrations).Assembly, shop);

app.MapSproutDB();       // POST /sproutdb/query
app.MapSproutDBHub();    // SignalR real-time changes at /sproutdb/changes
app.Run();
```

### As an Embedded Library

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

var db = sprout.SelectDatabase("shop");
```

### HTTP Usage

There is one endpoint: `POST /sproutdb/query`. The body is the query as `text/plain`. The `X-SproutDB-Database` header selects the database; `X-SproutDB-ApiKey` is only checked when auth is enabled (a `MasterKey` is configured — the sandbox in `sandbox/SproutDB.Sandbox` uses `sdb_ak_1234`).

Create the database (skip this if it already exists, e.g. via `GetOrCreateDatabase("shop")` — you'd get `DATABASE_EXISTS`):
```
POST /sproutdb/query
X-SproutDB-Database: shop
X-SproutDB-ApiKey: sdb_ak_1234
Content-Type: text/plain

create database
```

Create a table (same request, new body):
```
create table users (name string, email string 320 strict, age ubyte, active bool default true)
```

Insert a record:
```
upsert users {name: 'John', email: 'john@test.com', age: 25}
```

Query it back:
```
get users where active = true
```

Every response follows the same format — predictable, always JSON, always the same fields. Over HTTP the body is always a JSON **array** with one response per statement, even for a single query:

```json
[
  {
    "operation": 1,
    "data": [{"_id": 1, "name": "John", "email": "john@test.com", "age": 25, "active": true}],
    "affected": 1,
    "schema": null,
    "paging": null,
    "backup_path": null,
    "errors": null,
    "annotated_query": null
  }
]
```

No surprises. `operation` is an enum, `data` is your records, unused fields are `null`. Same object for gets, upserts, deletes, schema changes, and errors.

Need values from user input? Send `application/json` instead and let SproutDB bind `@name` placeholders — no escaping, no injection:

```
POST /sproutdb/query
X-SproutDB-Database: shop
X-SproutDB-ApiKey: sdb_ak_1234
Content-Type: application/json

{"query": "get users where email = @email", "parameters": {"email": "john@test.com"}}
```

### In-Process Usage

```csharp
// Create schema
db.Query("create table users (name string, email string 320 strict, age ubyte, active bool default true)");

// Insert
db.Query("upsert users {name: 'John', email: 'john@test.com', age: 25}");

// Query
var response = db.Query("get users where active = true")[0];
// response.Operation, response.Data, response.Affected — same format as HTTP

// With parameters
var john = db.Query("get users where email = @email", new { email = "john@test.com" })[0];
```

`db.Query` returns a `List<SproutResponse>` — one `SproutResponse` per statement, the same objects that make up the HTTP array. The typed LINQ API (`.Run()`, see below) returns a single `SproutResponse`.

---

## Features

**Human-Readable Query Language** — Not SQL. Three data commands: `get`, `upsert`, `delete`. Schema commands like `create table`, `add column`, `rename column`, `describe`. Single-quote strings so you don't fight C# escaping. Joins via `follow`, aggregation with `sum`/`avg`/`min`/`max`/`count` and `group by`, computed fields, paging — all built in. The query language reads like what you mean, not like what the database needs. The complete language reference lives in [`sproutdb-reference.md`](sproutdb-reference.md).

```
get users
    follow users._id -> orders.user_id as orders
    select name, orders.total
```

**Parameters** — `@name` placeholders for every value that comes from outside. Each value becomes exactly one literal — it can never turn into query syntax. In-process via `db.Query(query, new { ... })`, over HTTP via the JSON body shown above.

```csharp
db.Query("get users where name in @names", new { names = new[] { "John", "Jane" } });
```

**Multi-Query Batching** — Separate statements with `;` and send them in one go. You get one response per statement, in order.

```
get users where active = true; get orders; describe users
```

**Transactions** — Wrap writes in `atomic; ...; commit` and they succeed or fail together, with a real rollback. Reads inside the transaction see its own uncommitted writes.

```
atomic;
upsert accounts {_id: 1, balance: 50};
upsert accounts {_id: 2, balance: 150};
commit
```

**Real-Time Change Notifications** — Subscribe to any table via SignalR and get pushed the exact same response object that the HTTP caller received. No separate event model, no polling, no external message queue. Your Blazor app, your API, your background service — they all see changes the moment they happen.

The hub lives at `/sproutdb/changes` (`app.MapSproutDBHub()`). Call `Subscribe(database, table)` to join the group `{database}.{table}` (e.g. `shop.users`) and receive `OnChange` events. In-process, it's one line:

```csharp
using var sub = db.OnChange("users", response => Console.WriteLine(response.Affected));
```

**Error DX** — When something goes wrong, the error response includes your original query annotated with `##` comments at the exact position of each error. You see what's wrong, where it's wrong. Copy the annotated query, fix the marked spots, send again.

```
get users where agee ##column 'agee' does not exist## > 18
```

Multiple errors are collected when possible — you don't fix one just to discover the next.

**Migrations** — Schema migrations via `IMigration` interface with a fluent API. Each database tracks which migrations have been applied. Migrations run at startup for all existing databases, and on demand when you create a new tenant at runtime. Failed migration = server won't start. No half-applied schemas.

```csharp
public class CreateUsersTable : IMigration
{
    public int Order => 1;

    public void Up(ISproutDatabase db)
    {
        db.CreateTable("users")
            .AddColumn<string>("name", size: 255)
            .AddColumn<string>("email", size: 320, strict: true)
            .AddColumn<byte>("age")
            .AddColumn<bool>("active", defaultValue: "true")
            .Execute();
    }
}
```

**Multi-Tenant by Design** — Each database is a directory on disk. Creating a tenant is creating a database. New tenant at runtime? Create the database, run migrations, ready.

```csharp
var db = sprout.GetOrCreateDatabase($"tenant_{tenantId}");
sprout.Migrate(typeof(TenantMigrations).Assembly, db);
```

**Auto-Paging** — Results are automatically paged when they exceed the configured page size (default: 100). The response includes the total count and a ready-made query for the next page. Stateless — no server-side tracking.

```json
[
  {
    "operation": 1,
    "data": [...],
    "affected": 100,
    "paging": {
      "total": 1523,
      "page_size": 100,
      "page": 1,
      "next": "get users where active = true page 2 size 100",
      "next_cursor": null
    }
  }
]
```

Just send the `next` string as your next query. Two flavours:

- **Offset paging** — `get users page 2 size 20`. Page numbers, made for UI grids.
- **Cursor paging** — `get users after '0' limit 500`. Keyset paging over `_id`: the response carries `next_cursor` (the last `_id` of the page) and a matching `next` query, until `next_cursor` is `null`. Linear cost for a full scan and no skipped or duplicated rows while others write — made for syncing or loading a whole table.

**Auto-Index** — SproutDB tracks which columns appear in your `where` clauses, how selective they are, and whether the table is read-heavy or write-heavy. When the numbers make sense, it builds a B-Tree index in the background. When a column stops being queried, the index gets cleaned up after 30 days. You can also create indexes manually with `create index users.email` if you know what you need upfront.

**In-Process LINQ API** — First-class native support, no Entity Framework, no ORM layer in between. Expression trees are translated directly into SproutDB's internal query objects. Full IntelliSense, compile-safe, zero overhead.

Querying:
```csharp
var users = db.Table<User>("users");

// Returns SproutResponse — same object as HTTP and Query String
var response = users.Where(u => u.Age > 18).Run();

// Or get typed results directly
var adults = users.Where(u => u.Age > 18).ToList();
var john = users.FirstOrDefault(u => u.Id == 42);
var count = users.Where(u => u.Active).Count();
```

Upserting:
```csharp
// Insert with typed object (all fields)
users.Upsert(new User { Name = "John", Email = "john@test.com", Age = 25 });

// Partial update with anonymous object (only age changes)
users.Upsert(new { Id = 1ul, Age = (byte)26 });

// Upsert on match column
users.Upsert(new User { Email = "john@test.com", Name = "John Doe" }, on: u => u.Email);
```

**Upsert = Insert + Update** — There is no separate INSERT or UPDATE command. SproutDB figures it out. If there's no `_id`, it's an insert. If there's an `_id`, it's an update. You can also match on any column. Partial updates only touch the fields you provide — everything else stays untouched.

```
upsert users {name: 'John', email: 'john@test.com', age: 25}
## no id → insert, ID gets auto-generated

upsert users {_id: 1, age: 26}
## _id present → only age changes, name/email/active stay as they are

upsert users {email: 'john@test.com', name: 'John Doe'} on email
## email exists? update. doesn't? insert.
```

**Permissions** — Role-based access on database level. Three roles: `admin` (everything), `writer` (data operations), `reader` (queries only). Authentication via API keys in the `X-SproutDB-ApiKey` header, managed with queries. On top of that, single tables can be restricted per API key to `reader` or `none`:

```
create apikey 'reporting'
grant reader on shop to 'reporting'
restrict users to none for 'reporting' on shop
unrestrict users for 'reporting' on shop
```

**TTL** — Let data expire on its own, per table or per row. Expired rows are invisible immediately and cleaned up in the background.

```
create table sessions (token string 64 strict) ttl 24h
upsert sessions {token: 'abc123', ttl: 1h}
```

**Backup & Restore** — `backup` writes the whole database into a ZIP file, `restore 'path/to/backup.zip'` brings it back. The Admin UI offers the same as download and upload.

**Admin UI** — `builder.Services.AddSproutDBAdmin()` plus `app.MapSproutDBAdmin()` gives you a built-in web UI at `/sproutdb/admin`: query editor, schema browser, monitoring and API key management.

**Remote Client** — `AddSproutDBClient(o => o.BaseAddress = ...)` registers the same `ISproutServer` / `ISproutDatabase` interfaces against a remote SproutDB server over HTTP. Your code stays the same whether the database is embedded or across the network.

---

## Under the Hood

For those curious about the internals:

**Column-per-File Storage** — Each column is stored in its own memory-mapped file with fixed-size entries. A query that only needs `name` and `age` only reads `name.col` and `age.col` — the other columns are never touched. Schema changes are file operations: `add column` creates a new file, `purge column` deletes one. No table rebuilds, no downtime.

**Write Path** — All writes go through a single-writer queue (`Channel<T>`). Every write is appended to a write-ahead log before it is applied, then acknowledged. The WAL is fsynced in the background as a group commit (default every 50 ms, `WalSyncInterval`). If the process crashes, nothing is lost: the WAL is still in the OS cache and gets replayed on startup — idempotent. On a power loss or OS crash, acknowledged writes from the last sync interval can be lost; set `WalSyncInterval = TimeSpan.Zero` to fsync before every response instead (durable, but much lower throughput). The WAL stores the original query strings, so it's human-readable and format-stable across engine updates.

**Read Path** — Reads are lock-free and fully parallel, working directly on memory-mapped files. There's no buffer pool and no cache layer — the OS page cache handles everything. Hot data stays in RAM automatically, cold data gets paged in on demand.

**Auto-Indexing** — B-Tree index files sit next to column files (`email.btree` next to `email.col`). The engine monitors query patterns and builds indexes when usage frequency is high, selectivity is high, and the table is read-heavy. Index creation runs in the write queue so there are no concurrency issues. B-Tree updates on writes are O(log n) — negligible compared to the WAL append.

---

## Type System

All integer types require an explicit S/U prefix — signed or unsigned, you decide. No ambiguity, no wasted bytes.

| Type | .NET | Bytes |
|---|---|---|
| `sbyte` / `ubyte` | `sbyte` / `byte` | 1 |
| `sshort` / `ushort` | `short` / `ushort` | 2 |
| `sint` / `uint` | `int` / `uint` | 4 |
| `slong` / `ulong` | `long` / `ulong` | 8 |
| `float` / `double` | `float` / `double` | 4 / 8 |
| `bool` | `bool` | 1 |
| `string` | `string` | 255 default, configurable up to 1MB |
| `date` / `time` / `datetime` | `DateOnly` / `TimeOnly` / `DateTime` | 4 / 8 / 8 |
| `blob` | Base64 `string` | 8 + data in its own files |
| `array` | typed list, e.g. `array string 30` | 8 + data in its own files |

ID (`_id`) is always `ulong`, auto-increment, starts at 1. Every column has a fixed size on disk — you choose the type, you control the storage.

Type widening is allowed as long as the source type's full value range (min and max) fits in the target type. Narrowing is always an error.

---

## Project Status

SproutDB is a hobby project in active development.

Ideas, feedback, and discussions are very welcome — open an issue. Code contributions make sense once the project reaches a solid MVP status.

---

## License

MIT

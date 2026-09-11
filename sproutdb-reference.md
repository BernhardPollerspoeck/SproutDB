# SproutDB Reference

Kompakte Referenz für KI-Agenten. Alles aus dem Source Code belegt.

---

## In-Process Setup

### Service Registration

```csharp
using SproutDB.Core.DependencyInjection;

// Option A: Builder Callback
services.AddSproutDB(options =>
{
    options.DataDirectory = "/data/sproutdb";
    options.ChunkSize = 10_000;       // Default: 10000 (Slots pro Allokation)
    options.DefaultPageSize = 100;    // Default: 100
    options.BulkLimit = 100;          // Default: 100
    options.AddMigrations<MyMigrations>("database_name");
});

// Option B: aus IConfiguration ("SproutDB" Section)
services.AddSproutDB(configuration);

// Option C: Configuration + Code Override
services.AddSproutDB(configuration, builder =>
{
    builder.DataDirectory = "/override/path";
});
```

### Auth (opt-in)

```csharp
services.AddSproutDBAuth(o => o.MasterKey = "sdb_ak_...");
```

### HTTP + Admin

```csharp
var app = builder.Build();
app.MapSproutDB();        // POST /sproutdb/query
app.MapSproutDBHub();     // SignalR /sproutdb/changes
app.MapSproutDBAdmin();   // Blazor Admin UI /sproutdb/admin
```

### Direkter Engine-Zugriff (ohne DI)

```csharp
var engine = new SproutEngine(new SproutEngineSettings
{
    DataDirectory = "/data/sproutdb"
});
var db = engine.GetOrCreateDatabase("mydb");
var results = db.Query("get users"); // returns List<SproutResponse>
var result = results[0];
engine.Dispose();
```

### Interfaces

```csharp
// ISproutServer (Singleton)
ISproutDatabase GetOrCreateDatabase(string name);
ISproutDatabase SelectDatabase(string name);
IReadOnlyList<ISproutDatabase> GetDatabases();
void Migrate(Assembly assembly, ISproutDatabase database);

// ISproutDatabase
string Name { get; }
List<SproutResponse> Query(string query);  // always returns list (multi-query support)
ValueTask<List<SproutResponse>> QueryAsync(string query, CancellationToken ct = default);
IDisposable OnChange(string table, Action<SproutResponse> callback);

// Extensions (SproutDatabaseExtensions): Parameter + async
db.Query("… where k = @key", new { key });            // bzw. IReadOnlyDictionary<string, object?>
await db.QueryAsync("… where k = @key", new { key }, ct);

// QueryAsync: blockiert keinen Thread, während Writes in der Writer-Queue warten;
// reine Reads laufen synchron durch. Cancellation greift nur für Writes, die der
// Writer noch nicht begonnen hat → OperationCanceledException = kein Write dieses
// Aufrufs lief. Nach dem ersten Write läuft der Rest eines Batches durch.
// Der HTTP-Endpoint führt intern asynchron aus (ohne Abbruch bei Client-Disconnect).
void SaveQuery(string name, string query, bool pinned = false); // seedet _saved_queries (Admin UI)

// ISproutEntity (für Typed LINQ API db.Table<T>())
ulong Id { get; set; }  // gemappt auf _id
```

### Response

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

public sealed class PagingInfo
{
    public int Total { get; init; }
    public int PageSize { get; init; }
    public int Page { get; init; }        // 0 bei Cursor-Paging
    public string? Next { get; init; }    // fertige Folge-Query (Offset UND Cursor)
    public string? NextCursor { get; init; } // nur bei 'after'-Queries; null = letzte Seite
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

### Modi

| Modus | Verhalten |
|-------|-----------|
| `Once` | Läuft einmal, wird in `_migrations` Tabelle getrackt, bei erneutem Start übersprungen |
| `OnStartup` | Läuft bei jedem Start, wird NICHT getrackt. Für Cleanup-Tasks |

### Beispiel

```csharp
public sealed class CreateUsers : IMigration
{
    public int Order => 1;
    public void Up(ISproutDatabase db)
    {
        db.Query("create table users (name string 100, email string 320 strict, active bool default true)");
        db.Query("create index unique users.email");
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

### Registrierung

```csharp
services.AddSproutDB(options =>
{
    options.DataDirectory = "/data";
    options.AddMigrations<MyMigrations.Marker>("shop");
});
```

`SproutMigrationHostedService` führt Migrations VOR Kestrel aus. Bei Fehler → Server startet nicht.

---

## Typed LINQ API

### User-Klasse

Entity muss `ISproutEntity` implementieren (`Table<T>` braucht `T : class, ISproutEntity, new()`). `ISproutEntity` erzwingt `ulong Id { get; set; }` (gemappt auf `_id`).

```csharp
public sealed class User : ISproutEntity
{
    public ulong Id { get; set; }          // Pflicht via ISproutEntity, mappt auf _id
    public string Name { get; set; } = "";
    public string Email { get; set; } = "";
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

// Upsert — partial update mit anonymem Objekt
users.Upsert(new { Id = 1ul, Age = (byte)26 });

// Upsert on match column
users.Upsert(new User { Email = "john@test.com", Name = "John Doe" }, on: u => u.Email);

// Bulk Upsert
users.Upsert(new[] { user1, user2, user3 }, on: u => u.Email);

// Bedingtes Upsert (→ "on email when ..."): Fehlschlag = CONDITION_FAILED, Data = aktuelle Row
var r = users.Upsert(user, on: u => u.Email, when: u => u.Version == expectedVersion);
users.Upsert(user, on: u => u.Email, ifNotExists: true);   // → "when not exists"

// Delete
users.Delete(u => u.Active == false);
users.Delete(u => u.Id == 42);
users.Delete(u => u.Email == email && u.Version == v, expect: 1);  // → EXPECTATION_FAILED bei ≠ 1
```

Upsert/Delete werfen nicht, sondern liefern die `SproutResponse` — `CONDITION_FAILED` / `EXPECTATION_FAILED` über `response.Errors` prüfen.

---

## Datentypen

### Numerisch

| Typ | Größe | Wertebereich | Beschreibung |
|-----|-------|-------------|--------------|
| `sbyte` | 1 Byte | -128 bis 127 | Signed Byte |
| `ubyte` | 1 Byte | 0 bis 255 | Unsigned Byte |
| `sshort` | 2 Bytes | -32.768 bis 32.767 | Signed Short |
| `ushort` | 2 Bytes | 0 bis 65.535 | Unsigned Short |
| `sint` | 4 Bytes | ±2,1 Milliarden | Signed Integer |
| `uint` | 4 Bytes | 0 bis 4,2 Milliarden | Unsigned Integer |
| `slong` | 8 Bytes | ±9,2 Quintillionen | Signed Long |
| `ulong` | 8 Bytes | 0 bis 18,4 Quintillionen | Unsigned Long (ID-Typ) |
| `float` | 4 Bytes | 32-Bit IEEE 754 | Gleitkomma einfach |
| `double` | 8 Bytes | 64-Bit IEEE 754 | Gleitkomma doppelt |

### Text

| Typ | Größe | Beschreibung |
|-----|-------|--------------|
| `string` | 255 Bytes (default) | UTF-8 Text, zero-padded |
| `string N` | N Bytes (max 1.048.576) | String mit expliziter Größe |

### Datum/Zeit

| Typ | Größe | Format | Beschreibung |
|-----|-------|--------|--------------|
| `date` | 4 Bytes | `yyyy-MM-dd` | Nur Datum, gespeichert als DayNumber |
| `time` | 8 Bytes | `HH:mm:ss.ffff` | Nur Zeit, gespeichert als Ticks |
| `datetime` | 8 Bytes | `yyyy-MM-dd HH:mm:ss.ffff` | UTC, gespeichert als Ticks |

### Sonstige

| Typ | Größe | Beschreibung |
|-----|-------|--------------|
| `bool` | 1 Byte | `true` / `false` |
| `blob` | 8 Bytes (Counter) | Binärdaten, eigene Dateien auf Disk. Base64 bei Ein-/Ausgabe (`get`); die `upsert`-Antwort enthält nur die Byte-Länge (`long`) |
| `array` | 8 Bytes (Counter) | Typisiertes Array, eigene Dateien auf Disk. Syntax: `array string 30` |

### Literal-Werte in Queries

| Typ | Syntax | Beispiel |
|-----|--------|---------|
| String | Einfache Anführungszeichen | `'hello'`, `'it\'s'`, `'C:\\data\\'` |
| Integer | Ziffern | `42`, `-7` |
| Float | Ziffern mit Punkt | `3.14`, `-0.5` |
| Boolean | Keyword | `true`, `false` |
| Null | Keyword | `null` — in WHERE (`is null`) und als Upsert-Wert (`{payload: null}` leert die Spalte) |
| Datum | String-Format | `'2025-01-15'` |
| DateTime | String-Format | `'2025-01-15 14:30:00.0000'` |
| Duration (TTL) | Zahl + Einheit | `7d`, `24h`, `30m`, `45s` |

**Escapes in String-Literalen:** Es gibt genau zwei — `\'` → `'` und `\\` → `\`. Jeder andere Backslash bleibt wörtlich stehen (`'C:\temp'` = `C:\temp`). Ein Wert, der auf `\` endet, muss als `\\` geschrieben werden (`'a\\'` = `a\`), sonst maskiert der Backslash das schließende Hochkomma. Werte aus Code besser nie selbst einbauen, sondern als **Parameter** übergeben (nächster Abschnitt); wer doch Queries zusammenbaut: **erst `\` verdoppeln, dann `'` escapen**.

### Parametrisierte Abfragen

Statt Werte in den Query-Text zu bauen (und escapen zu müssen): `@name`-Platzhalter plus Parameter. Jeder Wert wird zu **genau einem Literal** — er kann nie zu Query-Syntax werden (keine Injection).

```csharp
db.Query("get grain_state where grain_key = @key", new { key = grainKey });
db.Query("upsert grain_state {grain_key: @k, etag: @next, payload: @p} on grain_key when etag = @expected",
    new { k = key, next = newEtag, p = payload, expected = oldEtag });
db.Query("get users where name in @names", new { names = new[] { "a", "b" } });
db.Query("get users where name = @name", new Dictionary<string, object?> { ["name"] = "x" });
```

- Nur an **Wert-Positionen**: WHERE-Werte, Upsert-Werte, `in @liste`, `when`-Bedingung. Nicht für Tabellen-/Spaltennamen (`get @table` → `SYNTAX_ERROR`).
- Namen sind case-insensitive; derselbe Platzhalter darf mehrfach vorkommen; gilt für alle Statements eines Batches.
- `@` in einem String-Literal (`'mail@x.com'`) ist kein Platzhalter.
- Typen: `string`, `char`, Zahlen, `bool`, `null`, `DateTime`/`DateTimeOffset`(UTC)/`DateOnly`/`TimeOnly`, `byte[]` (→ Base64 für Blob), Listen davon (→ `[...]`). Andere Typen → `ToString()` als String.
- Fehler → `PARAMETER_ERROR` (mit Position), bevor irgendetwas ausgeführt wird: fehlender Parameter, überzähliger Parameter, NaN/Infinity, verschachtelte Liste, Name doppelt (nur Groß/Klein verschieden).
- Ausgeführt und ins WAL geschrieben wird die gebundene Query (Werte als escapte Literale) — Replay parst exakt denselben Text.
- Die typisierte API (`db.Table<T>()`) nutzt intern Parameter.
- HTTP: JSON-Body `{"query": "...", "parameters": {...}}` (siehe HTTP API).

---

## Tabellen & Schema

### _id (Primary Key)

- Jede Tabelle hat automatisch eine `_id` Spalte (Typ: `ulong`)
- Auto-Inkrement, monoton steigend, wird nie recycled
- Kann NICHT manuell definiert werden
- Kann NICHT manuell gesetzt werden — `_id` im Upsert-Body ist nicht erlaubt

### Nullable & Defaults

- Spalten sind **standardmäßig nullable**
- `default VALUE` macht Spalte **nicht-nullable** und setzt den Default bei Insert
- Defaults werden NICHT rückwirkend auf bestehende Rows angewendet

### strict Modifier

- `strict` verhindert Typ-Änderungen auf der Spalte
- Schreibversuch mit breiterem Typ → `STRICT_VIOLATION` Error

### Namensregeln

- Erstes Zeichen: ASCII-Buchstabe (a-z, A-Z) oder `_`
- Folgezeichen: ASCII-Buchstabe, Ziffer (0-9) oder `_`
- `_`-Prefix ist für System-Tabellen reserviert (User kann nicht erstellen)
- Case-insensitive (intern lowercase)

---

## Beziehungen / "Foreign Keys"

SproutDB hat **keine klassischen Foreign Keys**. Beziehungen werden über **FOLLOW-Clauses** in GET-Queries abgebildet.

### Konzept

Man speichert die ID der referenzierten Tabelle manuell als Spalte:

```
create table users (name string 100)
create table orders (user_id ulong, product string 200, total double)
```

Es gibt **keine referentielle Integrität**. Keine Cascades. Keine automatische Validierung. Die Verknüpfung passiert ausschließlich zur Query-Zeit via `follow`.

### Datenmodell-Patterns

```
## 1:N Beziehung
create table users (name string)
create table orders (user_id ulong, amount double)
## Abfrage: follow users._id -> orders.user_id as orders

## N:1 Beziehung (Lookup)
create table orders (product_id ulong, quantity uint)
create table products (name string, price double)
## Abfrage: follow orders.product_id -> products._id as product

## M:N Beziehung (Junction Table)
create table users (name string)
create table roles (name string)
create table user_roles (user_id ulong, role_id ulong)
## get users
##     follow users._id -> user_roles.user_id as ur
##     follow ur.role_id -> roles._id as role
```

### Arrays / Listen

SproutDB hat einen `array`-Typ, aber für Beziehungen werden Junction Tables bevorzugt:

```
create table user_roles (user_id ulong, role string 30)
create index user_roles.user_id

upsert user_roles {user_id: 1, role: 'admin'}
upsert user_roles {user_id: 1, role: 'editor'}

get users
    follow users._id -> user_roles.user_id as roles
```

### Große Texte / Binärdaten

- **Blob** für große/variable Inhalte (Descriptions, Audio, Bilder)
- Blob = Base64 Ein/Ausgabe, eigene Datei auf Disk, **nicht durchsuchbar**
- **Nicht** `string 1048576` verwenden — Fixed-Size Rows verschwenden Platz bei kurzen Texten

### Enum-Felder

Status, Type, Role etc. als `string` mit fester Größe:

```
create table tickets (status string 20, priority string 10)
## Lesbar in Queries: where status = 'open'
```

---

## Query Language — Alle Befehle

### Kommentare

```
## Dies ist ein Kommentar ##
get users ## inline comment ## where active = true
## Kommentar bis Ende der Zeile (schließendes ## optional)
```

---

### Multi-Query & Transaktionen

```
## Batch: 3 Queries → 3 Responses (positional)
get users; get orders; describe users

## Transaktion: alles oder nichts, echter Rollback
atomic;
upsert accounts {_id: 1, balance: 50};
upsert accounts {_id: 2, balance: 150};
commit
```

- `atomic` und `commit` stehen je in einem eigenen Segment (eigenes Semikolon), keine Verschachtelung.
- **In `atomic` sind nur `upsert`, `delete`, `get` und `describe` erlaubt** — Schema-Änderungen (`create`, `add column`, `purge`, …), Backup/Restore und Auth-Befehle → `SYNTAX_ERROR`, weil ein Rollback sie nicht rückgängig machen kann. Tabellen/Indizes vorher anlegen (für Tabelle + Unique-Index atomar: `unique`-Modifier in `create table`).
- `get` / `describe` in der Transaktion sehen die eigenen, noch nicht committeten Writes.
- Jeder Fehler (inkl. `CONDITION_FAILED` / `EXPECTATION_FAILED`) rollt alles zurück; die Antwort ist dann ein einzelner Fehler mit `transaction rolled back: …`.
- Bei Erfolg endet die Response-Liste mit einem Transaction-Marker (`Operation = transaction`, `Affected` = Summe).

---

### CREATE DATABASE

```
create database
create database with chunk_size 500
```

Datenbankname kommt aus dem `X-SproutDB-Database` Header (HTTP) bzw. wird bei In-Process via `GetOrCreateDatabase("name")` gesetzt. `chunk_size` setzt den Default für alle Tabellen in dieser DB.

---

### CREATE TABLE

```
create table NAME
create table NAME (spalte1 typ [größe] [strict] [default wert] [unique], ...)
create table NAME (...) ttl DURATION
create table NAME (...) ttl DURATION with chunk_size N
create table NAME (...) with chunk_size N
```

Beispiele:
```
create table users (name string, email string 320 strict, age ubyte, active bool default true)
create table sessions (token string 64 strict) ttl 24h
create table products (name string 200, price double default 0)
create table admin_logs (msg string 500) with chunk_size 200
create table events (name string) ttl 7d with chunk_size 1000
```

- `chunk_size` steuert Slot-Preallokation (100–1.000.000). Default: Database → Engine (10.000)
- Reihenfolge: Columns → TTL → with chunk_size
- Modifier `strict` / `default` / `unique` in beliebiger Reihenfolge
- `unique` legt den Unique-Index **in derselben Anweisung** an wie die Tabelle (kein Zeitfenster „Tabelle ohne Index“, anders als `create table` + `create index unique`). Verhalten wie `create index unique` (NULLs erlaubt). Nicht für `blob`/`array` (`TYPE_MISMATCH`). Unique-Indizes werden vom Auto-Index-System nie entfernt.

```
create table grain_state (grain_key string 64 strict unique, etag string 32, payload blob)
```

---

### UPSERT (Insert / Update)

```
## Insert (neue ID wird generiert)
upsert users {name: 'John', email: 'john@test.com', age: 25}

## Bulk Insert
upsert users [{name: 'John', age: 25}, {name: 'Jane', age: 30}]

## Update by _id (on _id ist implizit)
upsert users {_id: 1, name: 'John Updated'}

## Upsert by Spalte (Insert wenn nicht vorhanden, Update wenn vorhanden)
upsert users {email: 'john@test.com', name: 'John'} on email

## Mit Row-TTL
upsert sessions {token: 'abc123', ttl: 24h}
upsert sessions [{token: 'a', ttl: 1h}, {token: 'b', ttl: 7d}]
```

**Verhalten:**
- Ohne `_id` und ohne `on` → Insert (neue ID wird auto-generiert)
- Mit `_id` im Body → implizit `on _id`, Update wenn ID existiert
- Mit `on COLUMN` → Lookup by Column-Wert: Update wenn gefunden, Insert wenn nicht. **Hat COLUMN einen Index (z.B. `unique`), läuft der Lookup über den B-Tree (O(log n)) — sonst über einen Scan aller Rows (O(n)).** Für Upsert-Keys daher immer einen (Unique-)Index anlegen.
- `_id` kann NICHT frei gewählt werden — immer auto-generiert bei Insert
- `{spalte: null}` leert die Spalte (nur bei nullable Spalten, sonst `NOT_NULLABLE`)
- TTL-Feld `ttl: DURATION` setzt Row-Ablaufzeit (`0` = keine Row-TTL, siehe [TTL](#ttl))
- Bulk-Limit: Default 100 Records pro Upsert (konfigurierbar)
- Antwort: `data` enthält die geschriebenen Rows inkl. `_id`. Blob-Spalten erscheinen dort als Byte-Länge (`long`), nicht als Base64 — den Inhalt liefert erst `get`

#### Bedingtes Upsert (`when`) — Compare-and-Set

```
## Update nur, wenn die gespeicherte Row die Bedingung erfüllt (optimistic concurrency / ETag)
upsert grain_state {grain_key: 'k', etag: 'E2', payload: '...'} on grain_key when etag = 'E1'

## Nur Insert — schlägt fehl, wenn die Row schon existiert
upsert grain_state {grain_key: 'k', etag: 'E1'} on grain_key when not exists

## Nur Update — kein Insert, wenn die Row fehlt
upsert grain_state {grain_key: 'k', etag: 'E2'} on grain_key when exists

## Volle WHERE-Grammatik, auch mit implizitem on _id
upsert grain_state {grain_key: 'k', etag: 'E2'} on grain_key when etag in ['E1', 'E1b'] and payload is not null
upsert grain_state {_id: 42, etag: 'E2'} when etag = 'E1'

## Grabstein mit TTL
upsert grain_state {grain_key: 'k', payload: null, etag: 'E2', ttl: 7d} on grain_key when etag = 'E1'
```

| Klausel | Row existiert, Bedingung wahr | Row existiert, Bedingung falsch | Row existiert nicht |
|---|---|---|---|
| `when <where-ausdruck>` | Update | `CONDITION_FAILED` | `CONDITION_FAILED` |
| `when exists` | Update | – | `CONDITION_FAILED` |
| `when not exists` | `CONDITION_FAILED` | – | Insert |
| ohne `when` | Update | – | Insert |

- Die Bedingung wird gegen die **gespeicherte** Row ausgewertet, die `on` (bzw. `_id`) findet — nicht gegen die neuen Werte.
- **Atomar:** Prüfung und Write laufen im Single-Writer im selben Schritt. Gilt embedded wie über HTTP mit beliebig vielen Clients/Prozessen — von zwei Writern mit demselben erwarteten ETag gewinnt genau einer.
- **Fehlerantwort `CONDITION_FAILED`:** `data` enthält die **komplette aktuelle Row** (wie `get`), damit der Aufrufer den gespeicherten ETag ohne zweiten Read kennt. Fehlt die Row, ist `data` leer. Die Fehlerposition zeigt auf `when`.
- **TTL:** Eine abgelaufene Row gilt als nicht vorhanden (auch wenn der Cleanup sie noch nicht gelöscht hat). `when not exists` räumt sie dann frei — sie wird physisch gelöscht — und fügt eine echte neue Row mit neuer `_id` ein, genau als wäre der Cleanup schon gelaufen.
- **In `atomic`:** `CONDITION_FAILED` rollt die ganze Transaktion zurück. `data` zeigt die Row so, wie sie **nach** dem Rollback gespeichert ist (nicht den Zwischenstand der Transaktion).
- **Nur Einzel-Records:** `when` bei Bulk-Upsert → `SYNTAX_ERROR` (mehrere bedingte Writes → `atomic` mit einzelnen Upserts).
- `when` braucht `on` oder `_id` im Record, sonst `SYNTAX_ERROR`. `when not exists` geht nur mit `on` (eine Row kann nicht mit explizitem `_id` angelegt werden).
- `exists` / `not exists` sind nur am Query-Ende Keywords — eine Spalte namens `exists` bleibt im Ausdruck nutzbar (`when exists = true`).

---

### GET

```
get TABLE
    [AGGREGATE column [as alias]]
    [select col1, col2 | LITERAL as alias | -select col1, col2]
    [distinct]
    [where WHERE]
    [count]
    [group by col1, col2]
    [order by col1 [desc], col2 [asc]]
    [limit N]
    [page N size M]
    [after 'CURSOR']
    [follow FOLLOW]*
    [select col1, follow_alias.col2]
```

Beispiele:
```
get users
get users where active = true and age >= 18
get users select name, email where active = true
get users -select password_hash
get users order by name asc, created desc
get users page 2 size 20
get users where active = true limit 10
get users where active = true count
get orders sum total as revenue where status = 'completed'
get orders avg total as average_order
get orders sum total as revenue group by status
get products select name, price * quantity as line_total
get orders select status distinct
get routes select host, true as preserve_host, 'auto' as protocol
get users
    follow users._id -> orders.user_id as orders
        where orders.status = 'completed'
get users
    follow users._id -> orders.user_id as orders
    select name, orders.total
```

#### Literal-Spalten (`LITERAL as alias`)

Konstante Werte projizieren — nützlich, um Result-Shapes zwischen Queries anzugleichen
(Discriminator-Felder, Defaults für Spalten, die es in dieser Tabelle nicht gibt).

```
get routes select host, true as preserve_host        ## bool
get routes select host, 'auto' as backend_protocol   ## string
get routes select host, 1 as version                 ## integer → long
get routes select host, 2.5 as factor                ## float → double
get routes select host, -1 as offset                 ## negative Zahl
get routes select host, null as cert_path            ## null
get routes select 1 as x                             ## ohne echte Spalte
```

**Regeln:**
- **Alias ist Pflicht** — `select host, 'auto'` → `SYNTAX_ERROR: literal in select requires an alias`
- Der Wert ist in **jeder** Zeile identisch — auch auf Zeilen, die ein Right/Outer-Join ohne Source-Row erzeugt
- Key-Reihenfolge folgt der Select-Liste: `select true as a, host` → `{ a, host }`
- Typen: `1` → long, `2.5` → double, `'x'` → string, `true`/`false` → bool, `null` → null

**`true` / `false` / `null` nur mit `as`:** diese drei sind Bezeichner-Tokens. Nur wenn ein `as`
folgt, sind sie Literale — sonst bleiben sie Spaltennamen.

```
get routes select true as x    ## Literal true
get routes select true         ## Spalte namens 'true' (→ UNKNOWN_COLUMN, wenn es sie nicht gibt)
```

**Nicht kombinierbar mit Aggregaten** — das Aggregat steht vor dem Select (`get t sum port as total`),
und der Parser überspringt den Select-Block, sobald er eins sieht. `select`/`-select` und Aggregate
schließen sich grammatikalisch aus.

**In `-select` nicht erlaubt** — `-select` entfernt Spalten nach Namen, ein Literal hat dort keine
Bedeutung → `SYNTAX_ERROR: literals are not allowed in '-select'`.

**Post-Follow:** Literale gehen auch im Select nach einem `follow`. Ein Basis-Literal überlebt einen
expliziten Post-Follow-Select nur, wenn es dort aufgelistet ist — genau wie eine Basis-Computed-Column.

```
get routes select host, 1 as v follow routes._id -> backends.route_id as b select host, b.name
  → v ist weg
get routes select host, 1 as v follow routes._id -> backends.route_id as b select host, b.name, v
  → v bleibt
```

Im Select **direkt am `follow`** (also für die Target-Tabelle) gibt es keine Literale: ein Literal
in der Liste markiert den Select als Post-Follow-Select. Das ist auch kein Verlust — eine Konstante
hängt nicht von der gejointen Zeile ab, `follow ... select 1 as x` wäre identisch zum Post-Follow.

#### Order By braucht die Spalte im Ergebnis

Sortiert wird auf den projizierten Zeilen. Eine Sortier-Spalte, die es in der Tabelle gibt, aber
nicht im Select steht, wird abgelehnt — früher lief die Query durch und sortierte still gar nicht.

```
get routes select host, port order by port desc   ## ok — port ist im Ergebnis
get routes order by port desc                     ## ok — ohne select sind alle Spalten da
get routes select host, port as p order by p      ## ok — Alias ist der Row-Key
get routes select host order by port desc         ## UNKNOWN_COLUMN: 'order by port' requires 'port' in the select list
get routes -select port order by port             ## Fehler — port wurde entfernt
get routes select host, port as p order by port   ## Fehler — der Key heißt jetzt 'p'
```

Ausnahmen (funktionieren ohne die Spalte im Select):
- `order by _id [desc] limit N` — eigener Top-N-Pfad, sortiert korrekt
- `order by _id` mit `after 'CURSOR'` — Cursor-Paging sortiert konstruktionsbedingt nach `_id`
- `count` — es kommen keine Zeilen zurück, Sortierung ist gegenstandslos

> **Breaking:** Queries wie `select host order by port` liefen früher fehlerfrei durch, kamen aber
> unsortiert zurück. Sie sind jetzt ein Fehler.

#### Doppelte Output-Namen

Ein Output-Name darf nur einmal vorkommen, sobald mindestens einer der kollidierenden Einträge
einen expliziten Alias trägt:

```
get routes select host, host                  ## ok — bloße Wiederholung
get routes select host, 1 as host             ## SYNTAX_ERROR: duplicate output name 'host'
get routes select host as x, port as x        ## SYNTAX_ERROR: duplicate output name 'x'
get orders select price * 2 as x, qty * 3 as x ## SYNTAX_ERROR: duplicate output name 'x'
get routes -select host, host                 ## ok — Exclude prüft nicht
```

> **Breaking:** `select price * 2 as x, qty * 3 as x` war früher erlaubt (der letzte Schreiber
> gewann stillschweigend) und ist jetzt ein Fehler.

#### Cursor-Paging (`after`)

Keyset-Paging über `_id` — für vollständige Tabellen-Durchläufe (z.B. Client-Hydration).
Statt Offset (`page N size M`) trägt der Client die letzte gesehene `_id` als Cursor:

```
get orders after '0' limit 500        ## erste Seite (ids > 0)
## Response.Paging: { next_cursor: '517', next: "get orders after '517' limit 500", ... }
get orders after '517' limit 500      ## nächste Seite
## ... bis next_cursor = null → fertig
```

**Verhalten:**
- Liefert Rows mit `_id > CURSOR`, sortiert nach `_id` aufsteigend
- `limit N` = Seitengröße; ohne `limit` gilt DefaultPageSize
- Response-`Paging`: `next_cursor` (letzte `_id` der Seite, `null` am Ende), `next` (fertige Folge-Query), `total` (verbleibende Treffer ab Cursor), `page` = 0
- Kombinierbar mit `where` und `select`; `order by _id` (asc) ist erlaubt (redundant)
- **Nicht** kombinierbar mit `page`, `count`, `distinct`, `group by`, Aggregaten, `follow` oder anderem `order by` → `SYNTAX_ERROR`
- Serveraufwand pro Seite: ein billiger ID-Scan + Projektion nur der Seitenzeilen → Voll-Durchlauf ist **linear** statt quadratisch
- **Korrekt unter parallelen Writes**: `_id` ist monoton und wird nie recycelt — Deletes/Inserts zwischen Seiten erzeugen keine übersprungenen oder doppelten Rows (Offset-Paging kann beides)

**Wann was:** `after` für vollständige Durchläufe/Hydration; `page N size M` für UI-Grids mit sichtbaren Seitenzahlen.

`order by _id [desc] limit N` ohne `after` nutzt denselben schnellen Pfad (Top-N ohne Voll-Materialisierung) — Ergebnis identisch, nur schneller.

---

### DELETE

```
delete TABLE where WHERE
delete TABLE where WHERE expect N
```

**WHERE ist Pflicht** (kein versehentliches Löschen aller Rows).

```
delete users where active = false
delete sessions where created < '2024-01-01 00:00:00.0000'

## Bedingtes Delete: nur löschen, wenn genau 1 Row passt (ETag-Check)
delete grain_state where grain_key = 'k' and etag = 'E1' expect 1
```

- `expect N` — passt eine andere Anzahl Rows als N, wird **nichts** gelöscht und `EXPECTATION_FAILED` zurückgegeben (Meldung nennt die gefundene Anzahl). In `atomic` rollt das die Transaktion zurück.
- Mit `expect` gelten abgelaufene TTL-Rows als nicht vorhanden: Sie werden weder gezählt noch gelöscht (das macht der TTL-Cleanup).
- Ohne `expect` meldet ein Delete einen Fehlschlag nur über `affected = 0`.

---

### DESCRIBE

```
describe            ## Alle Tabellen auflisten (+ DB chunk_size)
describe TABLE      ## Schema einer Tabelle (+ chunk_size, effective_chunk_size)
```

---

### SHRINK

```
shrink table TABLE
shrink table TABLE chunk_size N
shrink database
shrink database chunk_size N
```

- **shrink table**: Kompaktiert Index + Column Files, schließt Lücken (gelöschte Rows)
- **shrink table chunk_size N**: Zusätzlich neuen chunk_size setzen (für zukünftiges Wachstum)
- **shrink database**: Setzt DB-chunk_size, shrinkt alle Tabellen OHNE eigenen chunk_size
- Tabellen mit eigenem chunk_size werden bei `shrink database` übersprungen
- Target-Slots: `max(chunk_size, ceil(rows / chunk_size) * chunk_size)`
- Blockiert Schreibzugriffe während des Shrinks

---

### Spalten-Operationen

```
add column TABLE.SPALTE TYP [GRÖSSE] [strict] [default WERT]
rename column TABLE.ALTER_NAME to NEUER_NAME
alter column TABLE.SPALTE string NEUE_GRÖSSE
```

---

### Index-Operationen

```
create index TABLE.SPALTE
create index unique TABLE.SPALTE
purge index TABLE.SPALTE
```

**`unique` steht NACH `index`**, anders als in SQL (`CREATE UNIQUE INDEX`). Die SQL-Reihenfolge ist ein Syntax-Fehler: `create` akzeptiert als nächstes Token nur `database`, `table`, `index` oder `apikey`.

- Ohne `unique` erzwingt ein Index KEINE Eindeutigkeit — er beschleunigt nur Lookups
- Blob-Spalten können NICHT indexiert werden
- Indexes beschleunigen WHERE mit Equality/Range auf der indizierten Spalte

**Unique Index und NULL:** Ein Unique Index prüft ausschließlich Non-NULL-Werte. NULL wird von der Prüfung komplett übersprungen:

- **Beliebig viele Zeilen dürfen NULL** in einer Unique-Spalte haben — zwei NULLs sind keine Duplikate
- **Spalte im Upsert weglassen** überspringt die Prüfung genauso wie explizites NULL
- `create index unique` auf einer bestehenden Spalte **schlägt fehl**, wenn dort bereits doppelte Non-NULL-Werte liegen. Vorhandene NULLs verhindern das Anlegen nie.

```
create index unique users.email
upsert users {name: 'a', email: null}   ✓
upsert users {name: 'b', email: null}   ✓ zweites NULL ist ok
upsert users {name: 'c'}                ✓ Spalte weggelassen, keine Prüfung
upsert users {name: 'd', email: 'x@y'}  ✓
upsert users {name: 'e', email: 'x@y'}  ✗ UNIQUE_VIOLATION
```

Eine Spalte lehnt NULL nur ab, wenn sie ein `default` hat — nullable ist sie genau dann, wenn kein Default gesetzt ist. Ein `not null`-Keyword gibt es nicht, und `strict` betrifft nur Type-Widening, nicht Nullability.

> **Achtung:** `default` + `unique index` garantiert KEINE distinkten Werte pro Zeile. Defaults werden erst nach der Unique-Prüfung geschrieben — zwei Records, die die Spalte weglassen, bekommen denselben Default-Wert ohne `UNIQUE_VIOLATION`. Wert explizit setzen, wenn er eindeutig sein muss.

---

### Lösch-Operationen

```
purge table NAME
purge database
purge column TABLE.SPALTE
purge index TABLE.SPALTE
purge ttl TABLE
```

---

### TTL

```
## Table-Level TTL bei Erstellung
create table sessions (token string) ttl 24h

## Row-Level TTL bei Upsert
upsert sessions {token: 'abc', ttl: 1h}

## TTL entfernen
purge ttl sessions
```

Einheiten: `s` (Sekunden), `m` (Minuten), `h` (Stunden), `d` (Tage).

**Verhalten (darauf kann man sich verlassen):**
- Abgelaufene Rows sind **sofort unsichtbar** für `get` — auch bevor der Background-Cleanup sie physisch löscht (Cleanup läuft periodisch, Default alle 5 Minuten).
- Für `upsert … on COL` gilt eine abgelaufene Row ebenfalls als nicht vorhanden: Trifft `on` eine abgelaufene, noch nicht aufgeräumte Row, wird sie freigeräumt (physisch gelöscht) und der Record als **neue Row mit neuer `_id`** eingefügt — sie wird nie mit alten Werten „wiederbelebt“.
- **Jeder Upsert setzt die Ablaufzeit neu:** mit `ttl: X` gilt die Row-TTL X ab jetzt; ohne `ttl`-Feld (oder mit `ttl: 0`) gilt wieder die Tabellen-TTL — bzw. gar keine, wenn die Tabelle keine hat. Ein Update ohne `ttl` hebt eine Row-TTL also auf.
- `purge ttl TABLE` entfernt die Tabellen-TTL.

---

### Auth-Befehle

```
create apikey 'name'
purge apikey 'name'
rotate apikey 'name'
grant ROLE on DATABASE to 'apikey_name'
revoke DATABASE from 'apikey_name'
restrict TABLE to ROLE for 'apikey_name' on DATABASE
unrestrict TABLE for 'apikey_name' on DATABASE
```

Rollen: `admin`, `writer`, `reader`. Restrict nur `reader` oder `none`.
Key-Format: `sdb_ak_<32 random chars>`.
System-Tabellen: `_api_keys`, `_api_permissions`, `_api_restrictions` in `_system`.

---

### Backup & Restore

```
backup
restore 'pfad/zum/backup'
```

---

## WHERE-Clause — Referenz

### Vergleichsoperatoren

| Operator | Beschreibung |
|----------|-------------|
| `=` | Gleich |
| `!=` | Ungleich |
| `>` | Größer |
| `>=` | Größer oder gleich |
| `<` | Kleiner |
| `<=` | Kleiner oder gleich |
| `contains` | Substring (nur String) |
| `starts` | Prefix (nur String) |
| `ends` | Suffix (nur String) |
| `between X and Y` | Inklusiver Bereich |
| `not between X and Y` | Außerhalb des Bereichs |

### Null-Checks

```
column is null
column is not null
```

### Membership

```
column in [val1, val2, val3]
column not in [val1, val2, val3]
```

### Logische Operatoren

| Operator | Priorität | Beschreibung |
|----------|----------|-------------|
| `or` | Niedrigste | Disjunktion |
| `and` | Mittel | Konjunktion |
| `not` | Höchste | Negation (Prefix) |

### Beispiele

```
where active = true
where age >= 18 and age <= 65
where name contains 'john'
where status in ['active', 'pending']
where created between '2024-01-01' and '2024-12-31'
where not (status = 'deleted' or status = 'banned')
where email is not null and verified = true
```

**String-Vergleiche sind case-sensitive** (Byte-Level UTF-8 Vergleich).

---

## FOLLOW (Join) — Referenz

### Join-Typen

| Pfeil | Typ | Verhalten |
|-------|-----|-----------|
| `->` | Inner | Nur Rows mit Match in beiden Tabellen |
| `->?` | Left | Alle Source-Rows, NULL wenn kein Match |
| `?->` | Right | Alle Target-Rows, NULL wenn kein Match |
| `?->?` | Outer | Alle Rows aus beiden Tabellen |

### Syntax

```
follow SOURCE_TABLE.SOURCE_COL ARROW TARGET_TABLE.TARGET_COL as ALIAS
    [select col1, col2]
    [where BEDINGUNG]
```

### Beispiele

```
## Inner Join
get users
    follow users._id -> orders.user_id as orders

## Left Join (alle Users, auch ohne Orders)
get users
    follow users._id ->? orders.user_id as orders

## Mit Filter auf Follow-Tabelle
get users
    follow users._id -> orders.user_id as orders
        where orders.total > 100

## Mit Select auf Follow-Tabelle
get users
    follow users._id -> orders.user_id as orders
        select product, total

## Mehrere Follows (Chain)
get users
    follow users._id -> orders.user_id as orders
    follow orders.product_id -> products._id as product

## Post-Follow Select
get users
    follow users._id -> orders.user_id as orders
    select name, orders.total, orders.product
```

### Verhalten

- Follow expandiert Rows: 1 User mit 3 Orders → 3 Result-Rows
- Follow-Spalten werden mit Alias prefixed: `orders._id`, `orders.total`
- WHERE im Follow filtert die Target-Rows VOR dem Join
- Mehrere Follows werden sequenziell ausgeführt

---

## ChunkSize Kaskade

Jede Tabelle pre-allokiert Slots in Chunks. Die Chunk-Größe wird kaskadiert aufgelöst:

**Tabelle > Database > Engine-Global**

| Ebene | Wie setzen | Wann |
|-------|-----------|------|
| Engine-Global | `SproutEngineSettings.ChunkSize` (Default: 10.000) | Bereits vorhanden |
| Pro Database | `create database with chunk_size N` | Bei Erstellung |
| Pro Tabelle | `create table ... with chunk_size N` | Bei Erstellung |
| Nachträglich DB | `shrink database chunk_size N` | Jederzeit |
| Nachträglich Tabelle | `shrink table ... chunk_size N` | Jederzeit |

Wenn eine Tabelle keinen eigenen chunk_size hat (0), wird der Database-chunk_size verwendet. Hat die Database auch keinen (0), greift der Engine-Default.

---

## HTTP API

### Endpoint
```
POST /sproutdb/query
Content-Type: text/plain           ← Body = die Query
Content-Type: application/json     ← Body = {"query": "...", "parameters": {...}}
```

Mit JSON-Body werden `@name`-Platzhalter aus `parameters` gebunden (siehe [Parametrisierte Abfragen](#parametrisierte-abfragen)). Werte: String, Zahl, `true`/`false`, `null` oder ein Array davon — Objekte → 400.

```json
{"query": "upsert grain_state {grain_key: @k, etag: @next} on grain_key when etag = @expected",
 "parameters": {"k": "user/42", "next": "E2", "expected": "E1"}}
```

### Request Headers
| Header | Pflicht | Beschreibung |
|---|---|---|
| `X-SproutDB-Database` | Ja | Aktive Datenbank |
| `X-SproutDB-ApiKey` | Ja (wenn Auth aktiv) | API Key für Authentifizierung |

### Response Format

Response ist **immer ein JSON Array** von `SproutResponse` Objekten:
```json
// Single query: get users
[{"operation": 1, "data": [...], ...}]

// Multi query: get users; get orders
[{"operation": 1, "data": [...]}, {"operation": 1, "data": [...]}]

// Transaction: atomic; upsert A; upsert B; commit
[{"operation": 2, ...}, {"operation": 2, ...}, {"operation": 27, "affected": 2}]
```

### HTTP Status Codes

| Status | Wann |
|---|---|
| 200 | Query ausgeführt (Fehler stehen in den individuellen Responses) |
| 400 | Leerer Body, fehlender Database-Header oder ungültiger JSON-Body |
| 401 | Auth fehlend/ungültig (nur bei Auth-Middleware-Prüfung) |
| 403 | Keine Berechtigung (nur bei Auth-Middleware-Prüfung) |

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
| 15 | backup |
| 16 | restore |
| 17 | create_apikey |
| 18 | purge_apikey |
| 19 | rotate_apikey |
| 20 | grant |
| 21 | revoke |
| 22 | restrict |
| 23 | unrestrict |
| 24 | purge_ttl |
| 25 | shrink_table |
| 26 | shrink_database |
| 27 | transaction |

---

## SignalR

### Hub
```
Endpoint: /sproutdb/changes
Auth: X-SproutDB-ApiKey via Query String oder Header
```

### Subscribing

**Client → Server:**
| Methode | Beschreibung |
|---|---|
| `Subscribe(string database, string table)` | Tritt Group bei |
| `Unsubscribe(string database, string table)` | Verlässt Group |

**Server → Client:**
| Methode | Beschreibung |
|---|---|
| `OnChange(SproutResponse event)` | Selbes Response-Objekt wie HTTP |

### Gruppen
- Data-Changes: `{database}.{table}` (z.B. `shop.users`)
- Schema-Changes: `{database}._schema` (z.B. `shop._schema`)

### In-Process Change Notifications

```csharp
var sub = db.OnChange("users", response =>
{
    // response.Operation, response.Data, response.Affected etc.
});
sub.Dispose(); // unsubscribe
```

---

## Error Codes

| Code | Beschreibung |
|------|-------------|
| `SYNTAX_ERROR` | Query konnte nicht geparst werden |
| `UNKNOWN_TABLE` | Tabelle existiert nicht |
| `UNKNOWN_COLUMN` | Spalte existiert nicht |
| `UNKNOWN_DATABASE` | Datenbank existiert nicht |
| `TABLE_EXISTS` | Tabelle existiert bereits |
| `DATABASE_EXISTS` | Datenbank existiert bereits |
| `INDEX_EXISTS` | Index existiert bereits |
| `INDEX_NOT_FOUND` | Index existiert nicht |
| `TYPE_MISMATCH` | Wert passt nicht zum Spaltentyp |
| `NOT_NULLABLE` | NULL für nicht-nullable Spalte |
| `TYPE_NARROWING` | Typ-Verkleinerung nicht erlaubt |
| `STRICT_VIOLATION` | Typ-Erweiterung auf strict Spalte |
| `BULK_LIMIT` | Zu viele Records in Bulk-Upsert |
| `WHERE_REQUIRED` | DELETE braucht WHERE |
| `PROTECTED_NAME` | Name mit `_` Prefix (System-reserviert) |
| `UNIQUE_VIOLATION` | Unique-Index verletzt |
| `ID_NOT_FOUND` | Upsert mit `_id`, die nicht existiert |
| `CONDITION_FAILED` | `upsert … when …`: Bedingung nicht erfüllt — `data` enthält die aktuelle Row (leer, wenn keine) |
| `EXPECTATION_FAILED` | `delete … expect N`: andere Anzahl passender Rows, nichts gelöscht |
| `PARAMETER_ERROR` | `@name`-Parameter fehlt, ist überzählig oder nicht darstellbar — nichts ausgeführt |
| `AUTH_REQUIRED` | Kein API-Key angegeben |
| `AUTH_INVALID` | API-Key ungültig |
| `PERMISSION_DENIED` | Keine Berechtigung |
| `KEY_EXISTS` | API-Key Name existiert bereits |
| `KEY_NOT_FOUND` | API-Key nicht gefunden |

---

## Wichtige Verhaltensweisen

1. **Case-insensitive Identifiers** — Tabellen-/Spaltennamen werden intern lowercase
2. **Case-sensitive Strings** — WHERE-Vergleiche auf String-Spalten sind case-sensitive
3. **Fixed-Size Rows** — Alle Rows einer Tabelle haben gleiche Größe (Memory-Mapped Files)
4. **Single-Writer** — Alle Mutationen serialisiert über Channel-basierten Writer
5. **Lock-Free Reads** — GET-Queries laufen direkt auf Memory-Mapped Files
6. **Auto-Paging** — Ergebnisse > DefaultPageSize werden automatisch paginiert
7. **DELETE braucht WHERE** — Kein versehentliches Löschen aller Rows
8. **Keine Foreign Key Constraints** — Referentielle Integrität muss in der Applikation sichergestellt werden
9. **Blob = Base64** — Blob-Daten werden als Base64 ein-/ausgegeben, auf Disk als Einzeldateien
10. **Blob nicht indexierbar** — Create Index auf Blob-Spalten schlägt fehl
11. **NULL bei Unique-Index erlaubt** — Nur Non-NULL Werte müssen eindeutig sein
12. **Cursor-Paging für Voll-Durchläufe** — `after 'CURSOR'` (Keyset über `_id`) statt `page N size M`, wenn eine Tabelle komplett gelesen wird: linear, stabil unter parallelen Writes

---

## Speicherarchitektur

- **Column-per-File**: Jede Spalte = eigenes `.col` File mit Fixed-Size Entries
- **Index-File**: `_index` — Slot-basiert (20B Header + 8B pro Slot), ID → Place Mapping
- **Schema-File**: `_schema.bin` — Binär, Column-Definitionen + ChunkSize
- **Meta-File**: `_meta.bin` pro Database — CreatedTicks + ChunkSize
- **WAL**: Query-Strings + Sequence Numbers, human-readable, idempotent Replay (Auto-IDs werden mitgeschrieben, ein Replay-Insert bekommt dieselbe `_id`). Transaktionen: alle Einträge teilen eine groupId und werden erst durch einen abschließenden `commit`-Eintrag gültig — Replay überspringt Gruppen ohne Marker (zurückgerollt oder durch Crash unterbrochen)
- **TTL-File**: `_ttl` — 16B pro Slot (ExpiresAt + RowTtlDuration)
- **B-Tree**: `.btree` Files für manuelle und Auto-Indizes
- **Blob/Array**: `{col}_{id}.blob` / `{col}_{id}.array` — Einzeldateien pro Row
- **Slot-Wiederverwendung**: Gelöschte Slots werden neu belegt, sobald ≥ 20 % der Slots frei sind (Backfill). Eine Tabelle mit ständigem Delete + Insert wächst also nicht unbegrenzt — sie pendelt sich bei ca. Rows / 0,8 Slots ein. `shrink table` ist nur nötig, um Dateien nach großen Löschaktionen physisch zu verkleinern.

### Dauerhaftigkeit (WAL + Group Commit)

Jeder Write wird **vor** der Ausführung ins WAL geschrieben (OS-Buffer) und dann sofort bestätigt. Das `fsync` auf die Platte passiert gesammelt im Hintergrund (Group Commit), Default alle 50 ms (`WalSyncInterval`).

| Ereignis | Was geht verloren? |
|---|---|
| Prozess-Absturz (Exception, Kill, OOM) | **Nichts** — das WAL liegt bereits im OS-Cache und wird beim nächsten Start replayed |
| Stromausfall / OS-Crash | Bestätigte Writes der letzten ≤ `WalSyncInterval` (Default ≤ 50 ms) |

- `WalSyncInterval = TimeSpan.Zero` → `fsync` vor jeder Antwort (auch pro Transaktion). Dann ist jeder bestätigte Write auch bei Stromausfall sicher — kostet aber deutlich Durchsatz (ein `fsync` pro Write statt pro Batch).
- Konfiguration: `AddSproutDB(o => o.WalSyncInterval = TimeSpan.Zero)` bzw. `SproutDB:WalSyncIntervalMs` in der appsettings.
- Zusätzlich flusht der Flush-Cycle (`FlushInterval`, Default 5 s) die Datenfiles und leert danach das WAL.

### Auto-Index

Automatische B-Tree Indexbildung basierend auf Nutzungsmetriken:
- Nutzungshäufigkeit (>30% Queries), Selektivität (>95% Verwurf), Read/Write Ratio (>3:1)
- Auto-Entfernung nach 30 Tagen Nichtnutzung
- Manuell erstellte Indizes werden vom Auto-System nicht angefasst

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

app.MapSproutDB();          // POST /sproutdb/query
app.MapSproutDBHub();       // /sproutdb/changes
app.MapSproutDBAdmin();     // Blazor Admin UI

app.Run();
```

### Remote-Client (gleiche Schnittstelle wie embedded)

Ein Server wie oben, beliebig viele Clients (z.B. mehrere Orleans-Silos) — embedded oder remote ist nur eine Frage der Registrierung:

```csharp
// Embedded:  builder.Services.AddSproutDB(o => o.DataDirectory = "/data/sproutdb");
// Remote:
builder.Services.AddSproutDBClient(o =>
{
    o.BaseAddress = new Uri("https://db.example.com/");
    o.ApiKey = "sdb_ak_...";          // nur wenn der Server Auth hat
});

// Code bleibt gleich — ISproutServer ist jetzt ein SproutClient
var db = server.GetOrCreateDatabase("shop");   // bzw. SelectDatabase (wirft, wenn es sie nicht gibt)
db.Query("get users where email = @e", new { e = email });
await db.QueryAsync("upsert gs {k: @k, etag: @n} on k when etag = @o", new { k, n, o });
db.Table<User>("users").Where(u => u.Active).ToList();
using var sub = db.OnChange("users", r => …);   // SignalR, braucht MapSproutDBHub() am Server

// Ohne DI:
using var client = new SproutClient(new SproutClientOptions { BaseAddress = new Uri("https://db.example.com/") });
```

- Läuft über `POST /sproutdb/query` (`MapSproutDB()`); Parameter und typisierte API binden clientseitig, der Server sieht fertige Queries.
- `QueryAsync`: das Token greift nur bis die Anfrage gesendet ist — danach wird auf die Antwort gewartet, damit `OperationCanceledException` weiterhin „nichts ausgeführt“ bedeutet.
- `OnChange`: eine SignalR-Verbindung pro Abo (automatischer Reconnect, Abo wird erneuert).
- Row-Werte kommen als JSON-Primitive: `string`, `long` (bzw. `ulong`), `double`, `bool`, `null`, `List<object?>` — `_id` immer `ulong`. Die typisierte API konvertiert passend.
- **Nur am Server:** `GetDatabases()`, `Migrate(...)` (Migrations beim Server registrieren) und `SaveQuery(...)` → `NotSupportedException`.

### Mit Auth und Migrations
```csharp
builder.Services.AddSproutDB(options =>
{
    options.DataDirectory = "/data/sproutdb";
    options.AddMigrations<ShopMigrations.Marker>("shop");
});

builder.Services.AddSproutDBAuth(o => o.MasterKey = "sdb_ak_...");

var app = builder.Build();
app.MapSproutDB();
app.MapSproutDBHub();
app.MapSproutDBAdmin();
app.Run();
```

### appsettings.json
```json
{
  "SproutDB": {
    "DataDirectory": "/data/sproutdb",
    "DefaultPageSize": 100,
    "ChunkSize": 10000,
    "AutoIndex": {
      "Enabled": true,
      "UsageThresholdPercent": 30,
      "SelectivityThresholdPercent": 95,
      "ReadWriteRatioThreshold": 3.0,
      "UnusedIndexRemovalDays": 30
    }
  }
}
```

---

## Offene Punkte

### Übersicht

| Feature | Aufwand | Status |
|---------|---------|--------|
| Alias | ~6-8h | Designed |
| Full-Text Search | ~12-16h | Design fehlt |
| JSON Column Type | ~10-14h | Design fehlt |
| Auto-Index Monitoring UI | ~3-4h | Offen |
| Auto-Index Suggestions UI | ~4-5h | Offen |
| Autocomplete Lücken | ~8-10h | ~60% fertig |

### Implementiert

| Feature | Details |
|---------|---------|
| Multi-Query Batching | Semicolons als Delimiter, `Execute()` → `List<SproutResponse>` |
| Transactions | `atomic; ...; commit`, TransactionJournal mit MMF-Rollback, read-your-own-writes |
| Type Widening | Funktioniert (10 Tests grün) |

### Designed — bereit zur Implementierung

**Alias (~6-8h)**

Gespeicherte Query-Fragmente mit einem Namen. Auflösung auf AST-Ebene. Engine weiß nichts von Aliases — Pre-Processor vor der Engine.

```
create alias active_users as get users where active = true
purge alias active_users
load active_users where age > 25
load active_users
    follow users.id -> orders.user_id as orders
    select name, orders.total
```

Regeln:
- `create alias` überschreibt bei bestehendem Namen, `purge alias` löscht
- Nur `get`-Queries als Body, Validierung bei Erstellung
- Scope: pro Database, persistiert in `_aliases.idx` + `_aliases.dat`
- Verschachtelung erlaubt, Circle Detection
- Kombination mit `where`, `follow`, `select`, `distinct` am Aufruf
- Paging darf NICHT im Body stehen
- Execution: Nested (kein AST-Merge) — Alias komplett ausführen → materialisierte Rows → Aufrufer-Query operiert darauf

### Geplant — Design ausstehend

- **Full-Text Search** — Textsuche über String-Spalten (`where body search 'machine learning'`). Offene Fragen: Index-Typ, Tokenizer/Stemming, Fuzzy, Sprach-Support.
- **JSON Column Type** — Für semi-strukturierte Daten (Konfiguration, Metadaten)

### Admin UI

- [ ] Auto-Index Status auf Monitoring Page — Aktive Indizes mit auto/manual Badge
- [ ] Auto-Index Suggestions — Spalten die Schwellenwerte fast erreichen

### Autocomplete

Umfangreiche Test-Matrix mit ~70 Szenarien. Status: ~60% implementiert. Hauptsächlich fehlen:
- `and`/`or` nach WHERE-Bedingung
- Komma-getrennte Spalten (`select name, ⎸`)
- DELETE/DESCRIBE Autocomplete
- CREATE/PURGE Argument-Completion
- Auth-Befehle Completion
- Follow-Clause Completion

### Bekannte Einschränkungen

- **Numerische Typen großzügig wählen** — Type Widening (`ubyte` → `ushort` etc.) funktioniert via `add column table.col newtype`, aber erfordert File-Rebuild. Besser gleich den passenden Typ wählen.

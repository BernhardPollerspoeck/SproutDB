# SproutDB Query Language Design

## Grundprinzipien
- 3 Datenkommandos: `get`, `upsert`, `delete`
- 3 Schemakommandos: `add column`, `rename column`, `alter column`, `describe`
- 3 Strukturkommandos: `create database`, `create table`, `purge` (database/table/column)
- Jede Tabelle hat `id` (auto, wachsend, long/int)
- Alle Spalten sind typisiert und nullable by default
- Spalten mit `default` Wert sind nicht nullable
- Spalten mit `strict` verhindern Typ-Erweiterung
- Alles case-insensitive (intern lowercase)
- Spaltennamen: nur ASCII Buchstaben + Zahlen, muss mit Buchstabe starten
- Tabellen-/Datenbanknamen: selbe Regel wie Spaltennamen
- Prefix `_` ist reserviert für System-Objekte (`_system`, `_schema`). User-Namen dürfen nicht mit `_` starten.
- Kommentare: `##` (inline und block)

---

## Typsystem

### Datentypen
| Typ | .NET Mapping | Größe | Beschreibung |
|---|---|---|---|
| `string` | `string` | 255 Bytes default, konfigurierbar | Text, z.B. `string 5000` |
| `sbyte` | `sbyte` | 1 Byte | -128 bis 127 |
| `ubyte` | `byte` | 1 Byte | 0 bis 255 |
| `sshort` | `short` | 2 Bytes | -32.768 bis 32.767 |
| `ushort` | `ushort` | 2 Bytes | 0 bis 65.535 |
| `sint` | `int` | 4 Bytes | ±2.1 Mrd |
| `uint` | `uint` | 4 Bytes | 0 bis 4.2 Mrd |
| `slong` | `long` | 8 Bytes | ±9.2 Quintillionen |
| `ulong` | `ulong` | 8 Bytes | 0 bis 18.4 Quintillionen |
| `float` | `float` | 4 Bytes | Gleitkommazahl (32-bit) |
| `double` | `double` | 8 Bytes | Gleitkommazahl (64-bit) |
| `bool` | `bool` | 1 Byte | true/false |
| `date` | `DateOnly` | 4 Bytes | Datum, Format: `yyyy-MM-dd` |
| `time` | `TimeOnly` | 8 Bytes | Uhrzeit, Format: `HH:mm:ss.ffff` |
| `datetime` | `DateTime` | 8 Bytes | UTC, Format: `yyyy-MM-dd HH:mm:ss.ffff` |

> Jeder Typ hat eine feste Größe → alle Rows sind Fixed-Size.
> `string` ohne Längenangabe = 255 Bytes. Max: 1MB (`string 1048576`).
> Fixed-Size Rows ermöglichen direkten Zugriff per Row-Position, kein Index nötig.
> ID ist immer `ulong`, auto-increment, startet bei 1, nicht änderbar.
> S/U Prefix ist Pflicht bei allen Integer-Typen.

### Typ-Erweiterung
Nur erlaubt wenn der MaxValue des Quelltyps in den Zieltyp passt:
- `ubyte` → `ushort` → `uint` → `ulong` ✅
- `sbyte` → `sshort` → `sint` → `slong` ✅
- `ubyte` → `sint` ✅ (255 passt in ±2.1 Mrd)
- `uint` → `sint` ❌ (4.2 Mrd passt nicht in 2.1 Mrd)
- `sshort` → `ushort` ❌ (negative Werte passen nicht in unsigned)
- `float` → `double` ✅
- `double` → `float` ❌
- Verkleinern → immer Fehler

### Typsicherheit
```
sint 25       → sint
sint 'hello'  → Fehler!
```
> Kein `mixed`, kein `any`, kein `object`. Typ steht fest. Falscher Typ → Fehler.
> Bei `strict` Spalten: auch keine automatische Typ-Erweiterung bei Schema-Evolution.

### Nullable
- Jede Spalte ist nullable by default
- Spalten mit `default` Wert sind nicht nullable
- NULL setzen auf nicht-nullable Spalte → Fehler

---

## Kommentare
```
get users ##alle user## where age > 18 ##volljährig##
get users where active = true ## nur aktive
```
> `##` startet UND beendet einen Kommentar (inline toggle).
> Kommentare am Zeilenende brauchen kein schließendes `##`.
> Bei Query-Fehlern kann die DB Kommentare mit Fehlerinfos inline einfügen.

### Fehler-Feedback mit Kommentaren
```
## Anfrage
get users where agee > 18

## Response (Fehler)
get users where agee ##unknown column 'agee'## > 18
```
> Copy-Paste, fixen, absenden. Maximale DX.

---

## CREATE

### Datenbank erstellen
```
create database
```
> Erstellt die Database aus dem `X-SproutDB-Database` Header. Kein Argument – die DB steht IMMER im Header.

### Tabelle erstellen (minimal – nur Name, id kommt automatisch)
```
create table users
```

### Tabelle mit Spalten
```
create table users (name string, email string 320 strict, age ubyte, active bool default true, bio string 5000, created date)
```
> `id` wird immer automatisch angelegt.
> `string` ohne Länge = 255 Bytes. `string 5000` = 5000 Bytes.
> `strict` verhindert Typ-Erweiterung auf dieser Spalte.
> `default` setzt Standardwert und macht Spalte nicht-nullable.

---

## UPSERT

### Einfacher Insert (ID wird generiert)
```
upsert users {name: 'John', email: 'john@test.com', age: 25}
```

### Leerer Insert (nur ID)
```
upsert users {}
```

### Upsert mit Match-Spalte
```
upsert users {email: 'john@test.com', name: 'John Doe'} on email
```
> Existiert ein User mit dieser Email? → Update. Sonst → Insert.

### Upsert mit expliziter ID
```
upsert users {id: 42, name: 'John'}
```
> Update User 42 oder erstelle ihn mit ID 42.

### Partieller Update
```
upsert users {id: 42, name: 'New Name'}
```
> Ändert NUR `name`. Alle anderen Felder bleiben unverändert.
> Bei Insert: nicht angegebene Felder bekommen ihren Default oder null.
> NULL explizit setzen: `upsert users {id: 42, email: null}` (nur wenn Spalte nullable ist).

### Upsert Response
Jeder Upsert returnt die betroffenen Records im finalen State:
```
## Request
upsert users {email: 'john@test.com', name: 'John Doe'} on email

## Response
{
  "data": [{id: 42, email: 'john@test.com', name: 'John Doe', age: 25, active: true}],
  "affected": 1,
  "operation": "update"
}
```
> Kein extra GET nötig. Response enthält den kompletten Record inkl. aller bestehenden Felder.
> Bei Bulk Upsert kommen alle betroffenen Records zurück.
> `operation` ist `"insert"` oder `"update"` (bei Bulk eine Liste).

### Bulk Upsert
```
upsert users [
  {name: 'John', age: 25, email: 'john@test.com'},
  {name: 'Jane', age: 30, email: 'jane@test.com'},
  {name: 'Bob', age: 35, email: 'bob@test.com'}
] on email
```

---

## GET

### Alles aus einer Tabelle
```
get users
```

### Einzelne Spalten (Include)
```
get users select name, email
```

### Spalten ausschließen (Exclude)
```
get users -select age, email
```
> `-select` gibt alle Spalten zurück AUSSER den angegebenen.
> Entweder `select` oder `-select`, nie beides. Mischen → Fehler.
> `-select id` ist erlaubt – kein Sonderfall für ID.

### Where-Bedingungen
```
get users where age > 30
get users where name = 'John'
get users where email contains '@gmail'
get users where age >= 18 and active = true
get users where role = 'admin' or role = 'moderator'
get users where name starts 'Jo'
get users where name ends 'son'
get users where role in ['admin', 'moderator']
get users where role not in ['banned', 'suspended']
```

### Negation
```
get users where not active = true
get users where age not between 18 and 30
```

### NULL-Handling
```
get users where email is null
get users where email is not null
```

### Datum/Zeit
```
get users where created > '2025-01-01 14:30:00.0000'
get users where birthday > '2000-01-01'
get users where shift_start > '08:00:00.0000'
get users where created between '2025-01-01 00:00:00.0000' and '2025-06-01 00:00:00.0000'
```

### Sortierung
```
get users where age > 18 order by name
get users where age > 18 order by age desc
get users where active = true order by created desc, name
```

### Limit
```
get users where active = true limit 10
get users order by created desc limit 5
```

### Count
```
get users count
get users where active = true count
```

### Distinct
```
get users select city distinct
get users select role distinct where active = true
```

---

## GET - Aggregation

### Grundfunktionen
```
get orders sum amount
get orders avg amount
get orders min amount
get orders max amount
```

### Aggregation mit Alias
```
get orders sum amount as total_revenue
get orders avg amount as average_order_value where status = 'completed'
```

### Aggregation mit Filter
```
get orders sum amount where status = 'completed'
get orders avg amount where created > '2025-01-01 00:00:00.0000'
```

### Group By
```
get orders sum amount as revenue group by status
get orders count group by city
get orders avg amount as avg_amount group by customer_id order by avg_amount desc limit 10
```

### Computed Fields
```
get orders select amount, amount * 0.19 as tax
get orders select name, price, quantity, price * quantity as total
```

---

## GET - Joins (follow Syntax)

### Einfacher Join
```
get users follow users.id -> orders.user_id as orders
```

### Gefilterter Join
```
get users where active = true
  follow users.id -> orders.user_id as orders
  where orders.status = 'completed'
```

### Mehrere Joins
```
get users
  follow users.id -> orders.user_id as orders
  follow orders.product_id -> products.id as product
```

---

## DELETE

### Einzelner Record
```
delete users where id = 42
```

### Delete Response
Delete returnt die gelöschten Records:
```
## Request
delete users where active = false

## Response
{
  "data": [{id: 5, name: 'Old User', active: false}, {id: 12, name: 'Gone', active: false}],
  "affected": 2
}
```
> Komplette Records werden zurückgegeben – nützlich für Logging, Undo-UI, oder Auditing.

### Delete mit Bedingung
```
delete users where active = false
delete users where last_login < '2024-01-01'
delete sessions where status = 'expired'
```

---

## Schema Commands

### Spalte hinzufügen
```
add column users.premium bool
add column orders.priority int default 0
add column users.nickname string strict
```

### Spalte hinzufügen / erweitern
```
add column users.old_field string
```
> Spalte existiert nicht → anlegen.
> Spalte existiert mit kompatiblem Typ → erweitern (z.B. `ubyte` → `ushort`).
> Spalte existiert mit inkompatiblem Typ → Fehler.
> Spalte existiert mit gleichem Typ → stilles OK.

### Spalte umbenennen
```
rename column users.old_name to new_name
```

### String-Länge ändern
```
alter column users.bio string 10000
```
> Rebuilds das .col File mit neuer Entry Size. Reads laufen währenddessen weiter.
> Nur für String-Spalten. Vergrößern und Verkleinern erlaubt (Daten die nicht mehr passen werden abgeschnitten).

---

## PURGE (destruktiv, irreversibel)

### Spalte löschen
```
purge column users.old_field
```

### Tabelle löschen
```
purge table users
```

### Datenbank löschen
```
purge database
```
> Löscht die Database aus dem `X-SproutDB-Database` Header.
> Purge ist IMMER endgültig. Kein Soft-Delete, kein Undo. Weg ist weg.

---

## DESCRIBE

### Tabelle beschreiben
```
describe users
```
> Returnt alle Spalten mit Typ, nullable, default und ob strict (Typ-Erweiterung gesperrt).

### Alle Tabellen auflisten
```
describe
```
> Returnt alle Tabellen der aktuellen Datenbank.

### Describe Response (JSON)
```
## Request
describe users

## Response
{
  "table": "users",
  "columns": [
    {"name": "id", "type": "ulong", "nullable": false, "default": null, "strict": true, "auto": true},
    {"name": "name", "type": "string", "size": 255, "nullable": true, "default": null, "strict": false},
    {"name": "email", "type": "string", "size": 320, "nullable": true, "default": null, "strict": true},
    {"name": "age", "type": "ubyte", "size": 1, "nullable": true, "default": null, "strict": false},
    {"name": "active", "type": "bool", "size": 1, "nullable": false, "default": true, "strict": false}
  ],
  "row_size": 839
}
```

---

## Auto Paging & Limits

### Konfiguration
- **Default Page Size / Bulk Limit:** 100 (ein Setting für beides)
- Konfigurierbar auf Server-Ebene (global default)
- Override pro Database möglich

Wenn ein GET-Result die Page Size überschreitet, kommt Auto-Paging. Response enthält die fertige Query für die nächste Seite:
```
## Request
get users where active = true

## Response
{
  "data": [...],
  "paged": true,
  "total": 1523,
  "page_size": 100,
  "next": "get users where active = true page 2 size 100"
}

## Follow-Up: einfach den next-String senden
get users where active = true page 2 size 100
```
> Stateless. Kein serverseitiger State, kein Cursor-Tracking.
> `page` und `size` sind reguläre Syntax-Keywords, auch manuell nutzbar.
> Kein `next` wenn letzte Seite erreicht.

---

## Kombination / Komplexe Queries

### Top-Kunden mit Umsatz letzte 30 Tage
```
get orders sum amount as revenue group by customer_id
  where created > '2025-01-01 00:00:00.0000' and status = 'completed'
  order by revenue desc
  limit 10
```

### User mit Bestellhistorie, nur aktive Premium-User
```
get users where active = true and premium = true
  follow users.id -> orders.user_id as orders
  where orders.created > '2025-01-01 00:00:00.0000'
  order by users.name
```

### Upsert + sofort querien (Copy-Paste Workflow)
```
## Query Result:
[{id: 1, name: 'John', age: 25}, {id: 2, name: 'Jane', age: 30}]

## Direkt als Backup kopieren:
upsert users_backup [{id: 1, name: 'John', age: 25}, {id: 2, name: 'Jane', age: 30}] on id
```

---

## Entscheidungen
- Unbekannte Spalten bei Upsert → **Fehler**. Schema ist explizit, `add column` für neue Spalten.
- Typ-Erweiterung bei `add column` → nur erweitern erlaubt wenn MaxValue des Quelltyps in Zieltyp passt. Verkleinern/Inkompatibel → Fehler. Gleicher Typ → stilles OK.
- `last X days` Syntax → **raus**. Keine Magic. Explizite Datumswerte mit `between` oder Vergleichsoperatoren.
- Computed Fields Typ-Inferenz → immer der breiteste Typ (`sint * double` → `double`, `ubyte + uint` → `uint`).
- Database-Auswahl → **HTTP Header `X-SproutDB-Database`**. Hält Query-Syntax sauber, erweiterbar.
- Mehrere Queries pro Request → **Nein**. Eine Query pro Request. Kein Delimiter, Parser failt sauber bei unerwartetem Token nach Statement.
- `between` → **inklusiv** (wie SQL). `where age between 18 and 30` inkludiert 18 und 30.
- LINQ In-Process Modus → **later**. Beim Bauen berücksichtigen dass es gehen muss, Interface-Design kommt separat.
- WAL-Format → **Query-String**. Human-readable, format-stabil über Engine-Updates. Parser beim Replay akzeptabel.

---

## HTTP API

### Headers
| Header | Pflicht | Beschreibung |
|---|---|---|
| `X-SproutDB-Database` | Ja | Aktive Datenbank. Immer Pflicht – auch bei `create database` und `purge database`. |
| `X-SproutDB-ApiKey` | Ja (wenn Auth aktiv) | API Key für Authentifizierung |

### Endpoint
```
POST /query
Content-Type: text/plain
X-SproutDB-Database: shop
X-SproutDB-ApiKey: sdb_ak_xxxxx

get users where active = true
```
> Ein Endpoint für alles. Query als Plain Text im Body. Response immer JSON.

---

## Permissions (v1)

Basiert auf ASP.NET Authentication/Authorization:

### Auth-Methode
- API Key pro User (JWT Bearer als Alternative möglich)
- ASP.NET Middleware handelt Auth komplett

### Rollen
| Rolle | Rechte |
|---|---|
| `admin` | Alles: create/purge database/table, add/purge/rename column, upsert, delete, get, describe |
| `writer` | upsert, delete, get, describe |
| `reader` | get, describe |

### Scope
- v1: Rechte gelten auf **Database-Ebene**
- v2: Table-Level und Column-Level Permissions

---

## Error Response Format

Jeder Fehler kommt als JSON mit annotierter Query zurück. Die Query enthält `##`-Kommentare exakt an der Fehlerstelle – direkt fixen und nochmal senden.

### Struktur
```json
{
  "success": false,
  "errors": [
    {"code": "UNKNOWN_COLUMN", "message": "Column 'agee' does not exist in table 'users'"},
    {"code": "TYPE_MISMATCH", "message": "Expected ubyte, got string"}
  ],
  "annotated_query": "get users where agee ##unknown column 'agee'## > \'eighteen\' ##type mismatch: expected ubyte, got string##"
}
```

### Success Response (zum Vergleich)
```json
{
  "success": true,
  "data": [...],
  "affected": 5
}
```

### Error Codes
| Code | Beschreibung |
|---|---|
| `SYNTAX_ERROR` | Parser versteht die Query nicht |
| `UNKNOWN_TABLE` | Tabelle existiert nicht |
| `UNKNOWN_COLUMN` | Spalte existiert nicht |
| `TABLE_EXISTS` | Create auf existierende Tabelle |
| `DATABASE_EXISTS` | Create auf existierende Datenbank |
| `TYPE_MISMATCH` | Falscher Datentyp (z.B. String in Int-Spalte) |
| `NOT_NULLABLE` | NULL auf nicht-nullable Spalte |
| `TYPE_NARROWING` | Typ verkleinern nicht erlaubt (z.B. `double` → `float`, `uint` → `ubyte`) |
| `STRICT_VIOLATION` | Typ-Erweiterung auf strict-Spalte |
| `PERMISSION_DENIED` | Rolle hat keine Berechtigung |
| `BULK_LIMIT` | Bulk Upsert überschreitet konfiguriertes Limit |

### Annotierte Query Beispiele
```
## Tippfehler in Spalte
get users where agee ##unknown column 'agee'## > 18

## Typ-Fehler
upsert users {age: ##type mismatch: expected ubyte, got string## 'twenty-five'}

## Unbekannte Tabelle
get userss ##unknown table 'userss'## where active = true

## Syntax-Fehler
get users wher ##expected 'where', 'select', 'order', 'limit' or end of query## age > 18

## Nicht nullable
upsert users {id: 42, active: ##not nullable, default is 'true'## null}

## Mehrere Fehler auf einmal
get userss ##unknown table 'userss'## where agee ##unknown column 'agee'## > 'eighteen' ##type mismatch: expected ubyte, got string##
```
> `annotated_query` gibt es immer – auch bei Syntax-Fehlern wird die Position markiert.
> Mehrere Fehler werden gesammelt wenn möglich. Dead-Stop Fehler (z.B. Syntax) brechen sofort ab.

---

## SignalR Change Notifications

### Konzept
- Client subscribed auf eine **Table in einer Database** via SignalR Hub
- Group Name intern: `{database}.{table}` (z.B. `shop.users`)
- Kein Listener in der Group → kein Overhead (SignalR ignoriert leere Groups)
- Bei Disconnect räumt SignalR die Group-Membership automatisch auf

### Hub
```
Endpoint: /sproutdb/changes
Auth: X-SproutDB-ApiKey via Query String oder Header
```

### Hub-Methoden

**Client → Server:**
| Methode | Beschreibung |
|---|---|
| `Subscribe(string database, string table)` | Client tritt Group bei, empfängt Changes |
| `Unsubscribe(string database, string table)` | Client verlässt Group, kein Traffic mehr |

**Server → Client:**
| Methode | Beschreibung |
|---|---|
| `OnChange(ChangeEvent event)` | Wird an alle Clients in der Group gepusht |

> Permissions greifen: `reader` und `writer` können subscriben, nur auf Databases für die sie Zugriff haben.
> Subscribe/Unsubscribe laufen komplett über SignalR – nicht REST. Hub hat Auth-Context und Connection ID direkt verfügbar.

### Change Event
Das Event enthält exakt dieselbe Response wie der HTTP Request der die Änderung ausgelöst hat:
```json
{
  "data": [{id: 42, email: 'john@test.com', name: 'John Doe', age: 25, active: true}],
  "affected": 1,
  "operation": "update"
}
```
> Upsert, Delete – alle liefern dasselbe Format.
> Wer die Änderung macht sieht die Response via HTTP, alle anderen via SignalR. Selbes Objekt.

### Schema Change Events
Schema-Änderungen werden an die System-Group `{database}._schema` gepusht:
```json
{
  "operation": "add_column",
  "table": "users",
  "column": "premium",
  "type": "bool",
  "default": null,
  "nullable": true,
  "strict": false
}
```
> `add_column`, `purge_column`, `rename_column`, `purge_table`, `create_table` – alles kommt als Event.
> `create_table` und andere Events die vor der Table-Group existieren gehen an `{database}._schema`.
> Selbes Format via HTTP Response und SignalR. Immer 1:1 gleich.
> Client subscribed Schema-Events mit: `Subscribe("shop", "_schema")`

---

## Audit Log

### Scope
- **Geloggt:** Alle Schema-Änderungen (`create database`, `create table`, `add column`, `purge column`, `rename column`, `purge table`, `purge database`)
- **Nicht geloggt:** Daten-Operationen (`get`, `upsert`, `delete`)

### Log-Eintrag
```json
{
  "timestamp": "2026-02-24 14:30:00.0000",
  "user": "admin_api_key_name",
  "database": "shop",
  "query": "add column users.premium bool",
  "operation": "add_column"
}
```
> Audit Log wird in SproutDB selbst gespeichert (System-Database `_system`, Table `audit_log`).
> `_system` ist komplett read-only für alle User. Nur SproutDB intern schreibt.
> Abfragbar mit normalen GET Queries: `get audit_log` (mit Header `X-SproutDB-Database: _system`).


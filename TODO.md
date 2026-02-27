# SproutDB – Offene Punkte

Jeder Punkt ist ein Feature, Syntax-Element, Konzept oder Anforderung aus den Design-Dokumenten der noch nicht implementiert ist.

---

## Upsert

---

## Kommentare

---

## Fehlerbehandlung

---

## Schema

---

## Describe

---

## Get

- [x] #027 `get users -select age, email` – Exclude-Select. Gibt alle Spalten zurück AUSSER den angegebenen. Entweder `select` oder `-select`, nie beides (Mischen → Fehler). `-select id` ist erlaubt.
- [x] #028 `get users where age > 30` – Where-Clause mit Vergleichsoperator `>`.
- [x] #029 `get users where name = 'John'` – Where-Clause mit Gleichheit `=`.
- [x] #030 `get users where age >= 18` – Where-Clause mit `>=`.
- [x] #031 `get users where age <= 30` – Where-Clause mit `<=`.
- [x] #032 `get users where age < 18` – Where-Clause mit `<`.
- [x] #033 `get users where age != 18` – Where-Clause mit Ungleichheit (falls supported, aus Samples ableitbar).
- [ ] #034 `get users where email contains '@gmail'` – String-Operator `contains`.
- [ ] #035 `get users where name starts 'Jo'` – String-Operator `starts`.
- [ ] #036 `get users where name ends 'son'` – String-Operator `ends`.
- [ ] #037 `get users where age >= 18 and active = true` – Logischer Operator `and`.
- [ ] #038 `get users where role = 'admin' or role = 'moderator'` – Logischer Operator `or`.
- [ ] #039 `get users where not active = true` – Negation mit `not`.
- [ ] #040 `get users where role in ['admin', 'moderator']` – `in`-Operator mit Array-Literal.
- [ ] #041 `get users where role not in ['banned', 'suspended']` – `not in`-Operator.
- [ ] #042 `get users where age between 18 and 30` – `between`-Operator (inklusiv, wie SQL).
- [ ] #043 `get users where age not between 18 and 30` – `not between`.
- [ ] #044 `get users where email is null` – NULL-Check mit `is null`.
- [ ] #045 `get users where email is not null` – NULL-Check mit `is not null`.
- [ ] #046 Where mit Datum/Zeit-Vergleich: `get users where created > '2025-01-01 14:30:00.0000'`
- [ ] #047 Where mit Date-Vergleich: `get users where birthday > '2000-01-01'`
- [ ] #048 Where mit Time-Vergleich: `get users where shift_start > '08:00:00.0000'`
- [ ] #049 Where mit DateTime-Between: `get users where created between '2025-01-01 00:00:00.0000' and '2025-06-01 00:00:00.0000'`
- [ ] #050 `get users where age > 18 order by name` – ORDER BY (ascending default).
- [ ] #051 `get users where age > 18 order by age desc` – ORDER BY DESC.
- [ ] #052 `get users where active = true order by created desc, name` – Multi-Column ORDER BY.
- [ ] #053 `get users where active = true limit 10` – LIMIT.
- [ ] #054 `get users order by created desc limit 5` – LIMIT ohne WHERE.
- [ ] #055 `get users count` – COUNT (Anzahl Rows).
- [ ] #056 `get users where active = true count` – COUNT mit WHERE.
- [ ] #057 `get users select city distinct` – DISTINCT.
- [ ] #058 `get users select role distinct where active = true` – DISTINCT mit WHERE.
- [ ] #059 `get orders sum amount` – Aggregation SUM.
- [ ] #060 `get orders avg amount` – Aggregation AVG.
- [ ] #061 `get orders min amount` – Aggregation MIN.
- [ ] #062 `get orders max amount` – Aggregation MAX.
- [ ] #063 `get orders sum amount as total_revenue` – Aggregation mit Alias (AS).
- [ ] #064 `get orders avg amount as average_order_value where status = 'completed'` – Aggregation + Alias + WHERE.
- [ ] #065 `get orders sum amount where status = 'completed'` – Aggregation mit WHERE.
- [ ] #066 `get orders sum amount as revenue group by status` – GROUP BY.
- [ ] #067 `get orders count group by city` – COUNT mit GROUP BY.
- [ ] #068 `get orders avg amount as avg_amount group by customer_id order by avg_amount desc limit 10` – GROUP BY + ORDER BY + LIMIT kombiniert.
- [ ] #069 `get orders select amount, amount * 0.19 as tax` – Computed Fields mit Arithmetik und Alias.
- [ ] #070 `get orders select name, price, quantity, price * quantity as total` – Computed Fields mit mehreren Spalten.
- [ ] #071 Computed Fields Typ-Inferenz: immer der breiteste Typ (`sint * double` → `double`, `ubyte + uint` → `uint`).
- [ ] #072 `get users follow users.id -> orders.user_id as orders` – JOIN (follow Syntax).
- [ ] #073 Gefilterter Join: `get users where active = true follow users.id -> orders.user_id as orders where orders.status = 'completed'`
- [ ] #074 Mehrere Joins: `get users follow users.id -> orders.user_id as orders follow orders.product_id -> products.id as product`
- [ ] #075 Auto-Paging: Wenn GET-Result die Page Size überschreitet, kommt automatisch Paging. Response enthält `paging.total`, `paging.page_size`, `paging.page`, `paging.next` (fertige Query für nächste Seite).
- [ ] #076 `get users where active = true page 2 size 100` – Manuelle Paging-Syntax mit `page` und `size` Keywords.
- [ ] #077 Default Page Size / Bulk Limit: 100 (ein Setting für beides). Konfigurierbar auf Server-Ebene, Override pro Database möglich.
- [ ] #078 `get orders avg amount where created > '2025-01-01 00:00:00.0000'` – Aggregation + WHERE + Datums-Vergleich kombiniert.

---

## Delete

- [ ] #017 `delete users where age < 18` – DELETE mit WHERE. Löscht alle Rows die matchen. Response enthält `affected_rows`.
- [ ] #018 `delete users where id = 5` – DELETE by ID.
- [ ] #019 `delete users` (ohne WHERE) → Fehler `WHERE_REQUIRED`. Verhindert versehentliches Löschen aller Rows.
- [ ] #020 DELETE setzt Flag-Byte in jeder betroffenen Column auf 0x00 (gelöscht). Kein physisches Löschen, Platz wird bei nächstem Upsert wiederverwendet.
- [ ] #021 Free-List: Gelöschte Row-Positionen in einer Free-List (im Index-File oder separates File). Upsert prüft Free-List zuerst bevor neuer Platz alloziert wird.
- [ ] #022 DELETE + WAL: Delete-Query wird ins WAL geschrieben. Bei Replay: idempotent (bereits gelöschte Rows erneut löschen = no-op).

---

## Backup

---

## Migrations

- [ ] #079 ISproutServer Interface: `GetOrCreateDatabase()`, `SelectDatabase()`, `GetDatabases()`, `Migrate()`.
- [ ] #080 ISproutDatabase Interface: `Query()` für Query-String API.
- [ ] #081 IMigration Interface: `int Order { get; }` und `void Up(ISproutDatabase db)`. Migration weiß nicht gegen welche Database sie läuft.
- [ ] #082 Migration Tracking: `_migrations` Table pro Database (read-only für User). Felder: name, order, executed timestamp.
- [ ] #083 `sprout.Migrate(assembly, database)` – Scannt Assembly für IMigration, führt fehlende aus, schreibt in `_migrations`.
- [ ] #084 Migrations laufen VOR dem Öffnen der HTTP/SignalR Endpoints. Fehlgeschlagene Migration → Server startet nicht.

---

## Auto-Index

- [ ] #085 `create index users.email` – Manueller Index erstellen. B-Tree File (.btree) wird geschrieben.
- [ ] #086 `purge index users.email` – Manueller Index löschen.
- [ ] #087 B-Tree Index File-Format (.btree): mappt Column-Wert → Place. Kompakt, nur indexierter Wert + Place (Long).
- [ ] #088 Query Engine: hat die Spalte ein .btree? → B-Tree Lookup. Kein .btree? → .col File Scan.
- [ ] #089 B-Tree Update bei Writes: Bei jedem Insert/Update/Delete prüfen ob .btree existiert für betroffene Spalten → B-Tree Update (O(log n)).
- [ ] #090 Auto-Index: Usage Frequency Tracking – Counter wie oft Spalte in Where-Clauses genutzt wird, relativ zur Gesamtzahl Queries auf die Table.
- [ ] #091 Auto-Index: Selektivität Tracking – Verhältnis Rows gescannt vs. Rows im Result.
- [ ] #092 Auto-Index: Read/Write Ratio Tracking – Verhältnis Reads vs. Writes auf die Table.
- [ ] #093 Auto-Index: Entscheidungslogik – Hohe Nutzung + Hohe Selektivität + Read-Heavy → Index anlegen. Sonst → kein Index.
- [ ] #094 Auto-Index: Konfigurierbare Schwellwerte – Usage >30%, Selektivität >95% Verwurf, Read-Heavy >3:1 R/W Ratio. Alle Defaults konfigurierbar pro Server.
- [ ] #095 Auto-Index: Metriken in `_system.index_metrics` Table persistieren. Abfragbar mit normalen GET Queries.
- [ ] #096 Auto-Index: Index-Build in der Single-Writer Queue (sequentieller Scan über .col File → B-Tree aufbauen → .btree schreiben → Pointer-Swap).
- [ ] #097 Auto-Index: Ungenutzte Auto-Indizes nach 30 Tagen entfernen (konfigurierbar). Manuell erstellte Indizes werden nicht angefasst.
- [ ] #098 Auto-Index: Logging warum ein Index erstellt oder entfernt wurde.

---

## Fluent API

- [ ] #099 Fluent API: `db.CreateTable("users").AddColumn<string>("name").AddColumn<int>("age")` etc.
- [ ] #100 Fluent API: `db.AddColumn<bool>("users", "premium", defaultValue: false)`.
- [ ] #101 Fluent API: `db.AlterColumn("users", "bio", size: 10000)`.

---

## LINQ API

- [ ] #102 Typed LINQ API: `db.Table<T>("users")` – Table-Accessor mit generischem Typ.
- [ ] #103 LINQ: `.Where(u => u.Age > 18)` – Expression Tree → internes Query-Objekt.
- [ ] #104 LINQ: `.Select(u => u.Name)` – Projection.
- [ ] #105 LINQ: `.FirstOrDefault(u => u.Id == 42)` – Single Record.
- [ ] #106 LINQ: `.Count()` – Zählen.
- [ ] #107 LINQ: `.OrderBy()` / `.OrderByDescending()` – Sortierung.
- [ ] #108 LINQ: `.Take(10)` – Limit.
- [ ] #109 LINQ: `.Run()` – Returns SproutResponse (same as HTTP/Query String).
- [ ] #110 LINQ: `.ToList()` – Typed Results.
- [ ] #111 LINQ: `users.Upsert(new User { ... })` – Typed Insert/Update.
- [ ] #112 LINQ: `users.Upsert(new { Id = 1ul, Age = (byte)26 })` – Partial Update mit anonymem Objekt.
- [ ] #113 LINQ: `users.Upsert(new User { ... }, on: u => u.Email)` – Upsert mit Match-Column.
- [ ] #114 LINQ: `users.Upsert(new[] { user1, user2 }, on: u => u.Email)` – Bulk Upsert.
- [ ] #115 LINQ: `users.Delete(u => u.Active == false)` – Typed Delete.

---

## Unkategorisiert

- [ ] #116 `_system` Database: System-Database, komplett read-only für alle User. Nur SproutDB intern schreibt.
- [ ] #117 Audit Log: Alle Schema-Änderungen werden in `_system.audit_log` geloggt. Felder: timestamp, user, database, query, operation. NICHT geloggt: Daten-Operationen (get, upsert, delete).
- [ ] #118 Audit Log abfragbar: `get audit_log` mit Header `X-SproutDB-Database: _system`.
- [ ] #119 HTTP Server: `POST /query` Endpoint. Content-Type: text/plain. Query als Plain Text im Body, Response immer JSON.
- [ ] #120 HTTP Header `X-SproutDB-Database` (Pflicht) – Aktive Datenbank.
- [ ] #121 HTTP Header `X-SproutDB-ApiKey` (Pflicht wenn Auth aktiv) – API Key für Authentifizierung.
- [ ] #122 HTTP Status Code Mapping: 200 OK, 400 Query-Fehler, 401 Auth fehlend, 403 Keine Berechtigung, 404 Not Found, 409 Conflict.
- [ ] #123 SproutDB.Server Projekt: ASP.NET Host mit Kestrel.
- [ ] #124 `MapSproutDB()` Extension Method – registriert POST /query Endpoint.
- [ ] #125 `MapSproutDBHub()` Extension Method – registriert SignalR Hub auf `/sproutdb/changes`.
- [ ] #126 `AddSproutDB(options => ...)` DI Extension – registriert Engine im DI Container mit Settings.
- [ ] #127 `AddSproutDBAuth(options => ...)` DI Extension – aktiviert Auth.
- [ ] #128 appsettings.json Konfiguration: DataDirectory, DefaultPageSize, WalFlushIntervalSeconds, PreAllocateChunkSize, AutoIndex Settings, Auth Settings.
- [ ] #129 Docker Compose Support.
- [ ] #130 SignalR Hub: `/sproutdb/changes` Endpoint. Auth via `X-SproutDB-ApiKey` per Query String oder Header.
- [ ] #131 SignalR: `Subscribe(string database, string table)` – Client tritt Group bei, empfängt Changes.
- [ ] #132 SignalR: `Unsubscribe(string database, string table)` – Client verlässt Group.
- [ ] #133 SignalR: `OnChange(SproutResponse event)` – Server → Client Push. Selbes Response-Objekt wie HTTP.
- [ ] #134 SignalR Groups: Data-Changes auf `{database}.{table}`, Schema-Changes auf `{database}._schema`.
- [ ] #135 SignalR: Kein Listener in Group → kein Overhead. Bei Disconnect räumt SignalR Group-Membership auf.
- [ ] #136 SignalR: Permissions greifen – nur Databases für die der API Key Zugriff hat.
- [ ] #137 Schema Change Events an `{database}._schema` Group: `add_column`, `purge_column`, `rename_column`, `purge_table`, `create_table`.
- [ ] #138 Permissions: 3 Rollen – `admin` (alles), `writer` (upsert, delete, get, describe), `reader` (get, describe).
- [ ] #139 Permissions: API Key pro User (JWT Bearer als Alternative möglich). ASP.NET Middleware handelt Auth.
- [ ] #140 Permissions: v1 auf Database-Ebene, v2 Table-Level und Column-Level.
- [ ] #141 Error Code `AUTH_REQUIRED` – Auth fehlend.
- [ ] #142 Error Code `AUTH_INVALID` – Auth ungültig.
- [ ] #143 Error Code `PERMISSION_DENIED` – Rolle hat keine Berechtigung.
- [ ] #144 In-Process Change Notifications: `users.OnChange(change => { ... })` – Selbes Event wie SignalR, nur als Callback ohne Serialisierung.
- [ ] #145 Pre-Allocation Chunk Size konfigurierbar machen. Aktuell hardcoded 10.000 in `StorageConstants`. Soll über `SproutEngineSettings.PreAllocateChunkSize` steuerbar sein.

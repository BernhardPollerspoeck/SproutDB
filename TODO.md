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

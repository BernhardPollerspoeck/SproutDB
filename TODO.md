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

---

## Backup

---

## Migrations

- [x] #079 ISproutServer Interface: `GetOrCreateDatabase()`, `SelectDatabase()`, `GetDatabases()`, `Migrate()`.
- [x] #080 ISproutDatabase Interface: `Query()` für Query-String API.
- [x] #081 IMigration Interface: `int Order { get; }` und `void Up(ISproutDatabase db)`. Migration weiß nicht gegen welche Database sie läuft. Zusätzlich: `MigrationMode Mode` (Once/OnStartup).
- [x] #082 Migration Tracking: `_migrations` Table pro Database (read-only für User). Felder: name, order, executed timestamp.
- [x] #083 `sprout.Migrate(assembly, database)` – Scannt Assembly für IMigration, führt fehlende aus, schreibt in `_migrations`.
- [ ] #084 Migrations laufen VOR dem Öffnen der HTTP/SignalR Endpoints. Fehlgeschlagene Migration → Server startet nicht.

---

## Auto-Index

---

## Fluent API

---

## LINQ API

---

## Tests

- [ ] Test-Suite optimieren — dauert aktuell 6-10 Minuten, muss deutlich schneller werden
- [ ] `_id` Edge Cases prüfen und testen
- [ ] Reflection im MigrationRunner prüfen und entfernen (aktuell: `assembly.GetTypes()` + `Activator.CreateInstance` — Alternative: Source Generator oder explizite Registrierung)

---

## HTTP Server

- [ ] #119 `POST /query` Endpoint. Content-Type: text/plain. Query als Plain Text im Body, Response immer JSON.
- [ ] #120 HTTP Header `X-SproutDB-Database` (Pflicht) – Aktive Datenbank.
- [ ] #121 HTTP Header `X-SproutDB-ApiKey` (Pflicht wenn Auth aktiv) – API Key für Authentifizierung.
- [ ] #122 HTTP Status Code Mapping: 200 OK, 400 Query-Fehler, 401 Auth fehlend, 403 Keine Berechtigung, 404 Not Found, 409 Conflict.
- [ ] #123 SproutDB.Server Projekt: ASP.NET Host mit Kestrel.
- [ ] #124 `MapSproutDB()` Extension Method – registriert POST /query Endpoint.

---

## SignalR

- [ ] #125 `MapSproutDBHub()` Extension Method – registriert SignalR Hub auf `/sproutdb/changes`.
- [ ] #130 SignalR Hub: `/sproutdb/changes` Endpoint. Auth via `X-SproutDB-ApiKey` per Query String oder Header.
- [ ] #131 `Subscribe(string database, string table)` – Client tritt Group bei, empfängt Changes.
- [ ] #132 `Unsubscribe(string database, string table)` – Client verlässt Group.
- [ ] #133 `OnChange(SproutResponse event)` – Server → Client Push. Selbes Response-Objekt wie HTTP.
- [ ] #134 SignalR Groups: Data-Changes auf `{database}.{table}`, Schema-Changes auf `{database}._schema`.
- [ ] #135 Kein Listener in Group → kein Overhead. Bei Disconnect räumt SignalR Group-Membership auf.
- [ ] #136 Permissions greifen – nur Databases für die der API Key Zugriff hat.
- [ ] #137 Schema Change Events an `{database}._schema` Group: `add_column`, `purge_column`, `rename_column`, `purge_table`, `create_table`.
- [ ] #144 In-Process Change Notifications: `users.OnChange(change => { ... })` – Selbes Event wie SignalR, nur als Callback ohne Serialisierung.

---

## Auth & Permissions

- [ ] #127 `AddSproutDBAuth(options => ...)` DI Extension – aktiviert Auth.
- [ ] #138 3 Rollen – `admin` (alles), `writer` (upsert, delete, get, describe), `reader` (get, describe).
- [ ] #139 API Key pro User (JWT Bearer als Alternative möglich). ASP.NET Middleware handelt Auth.
- [ ] #140 v1 auf Database-Ebene, v2 Table-Level und Column-Level.
- [ ] #141 Error Code `AUTH_REQUIRED` – Auth fehlend.
- [ ] #142 Error Code `AUTH_INVALID` – Auth ungültig.
- [ ] #143 Error Code `PERMISSION_DENIED` – Rolle hat keine Berechtigung.

---

## Config & Deployment

- [x] #126 `AddSproutDB(options => ...)` DI Extension – SproutEngineSettingsBuilder + SproutServiceCollectionExtensions. Registriert SproutEngine + ISproutServer als Singleton.
- [ ] #128 appsettings.json Konfiguration: DataDirectory, DefaultPageSize, WalFlushIntervalSeconds, PreAllocateChunkSize, AutoIndex Settings, Auth Settings.
- [x] #145 Pre-Allocation Chunk Size konfigurierbar: `SproutEngineSettings.ChunkSize`. Durchgereicht durch TableCache → TableHandle → ColumnHandle/IndexHandle → CreateTableExecutor.

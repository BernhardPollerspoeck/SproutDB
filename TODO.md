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

- [x] #119 `POST /query` Endpoint. Content-Type: text/plain. Query als Plain Text im Body, Response immer JSON.
- [x] #120 HTTP Header `X-SproutDB-Database` (Pflicht) – Aktive Datenbank.
- [x] #121 HTTP Header `X-SproutDB-ApiKey` (Pflicht wenn Auth aktiv) – API Key für Authentifizierung.
- [x] #122 HTTP Status Code Mapping: 200 OK, 400 Query-Fehler, 401 Auth fehlend, 403 Keine Berechtigung, 404 Not Found, 409 Conflict.
- [x] #123 SproutDB.Server Projekt: ASP.NET Host mit Kestrel.
- [x] #124 `MapSproutDB()` Extension Method – registriert POST /query Endpoint.

---

## SignalR

- [x] #125 `MapSproutDBHub()` Extension Method – registriert SignalR Hub auf `/sproutdb/changes`.
- [x] #130 SignalR Hub: `/sproutdb/changes` Endpoint. Auth via `X-SproutDB-ApiKey` per Query String oder Header.
- [x] #131 `Subscribe(string database, string table)` – Client tritt Group bei, empfängt Changes.
- [x] #132 `Unsubscribe(string database, string table)` – Client verlässt Group.
- [x] #133 `OnChange(SproutResponse event)` – Server → Client Push. Selbes Response-Objekt wie HTTP.
- [x] #134 SignalR Groups: Data-Changes auf `{database}.{table}`, Schema-Changes auf `{database}._schema`.
- [x] #135 Kein Listener in Group → kein Overhead. Bei Disconnect räumt SignalR Group-Membership auf.
- [x] #136 Permissions greifen – nur Databases für die der API Key Zugriff hat.
- [x] #137 Schema Change Events an `{database}._schema` Group: `add_column`, `purge_column`, `rename_column`, `purge_table`, `create_table`.
- [x] #144 In-Process Change Notifications: `users.OnChange(change => { ... })` – Selbes Event wie SignalR, nur als Callback ohne Serialisierung.

---

## Auth & Permissions

Design-Dokument: `sproutdb-auth-design.md`

### DI & Middleware
- [x] #127 `AddSproutDBAuth(options => { options.MasterKey = "..."; })` DI Extension – aktiviert Auth.
- [x] #121 `X-SproutDB-ApiKey` Header im Endpoint auswerten (Pflicht wenn Auth aktiv).
- [x] #141 Error Code `AUTH_REQUIRED` (401) – Kein API Key mitgegeben.
- [x] #142 Error Code `AUTH_INVALID` (401) – API Key unbekannt.
- [x] #143 Error Code `PERMISSION_DENIED` (403) – Rolle hat keine Berechtigung.

### Rollen & Permissions
- [x] #138 3 Rollen – `admin` (alles), `writer` (upsert, delete, get, describe), `reader` (get, describe). Rolle immer pro Database, keine globale Rolle.
- [x] #140 Table-Level Permissions – Restrict-only (nie expanden). `reader` oder `none`.

### System-Tabellen
- [x] #146 `_api_keys` Tabelle in `_system` – name, key_prefix, key_hash, created_at, last_used_at.
- [x] #147 `_api_permissions` Tabelle in `_system` – key_name, database, role.
- [x] #148 `_api_restrictions` Tabelle in `_system` – key_name, database, table, role.

### Query-Syntax (Parser)
- [x] #149 `create apikey '<name>'` – Key erstellen (nur MasterKey).
- [x] #150 `purge apikey '<name>'` – Key löschen (nur MasterKey).
- [x] #151 `rotate apikey '<name>'` – Key rotieren, selbe Permissions (nur MasterKey).
- [x] #152 `grant <role> on <db> to '<name>'` – DB-Zugriff gewähren (MasterKey + admin).
- [x] #153 `revoke <db> from '<name>'` – DB-Zugriff entziehen (MasterKey + admin).
- [x] #154 `restrict <table|*> to <reader|none> for '<name>' on <db>` – Table einschränken (MasterKey + admin).
- [x] #155 `unrestrict <table> for '<name>' on <db>` – Restriction entfernen (MasterKey + admin).

### Key-Format
- [x] #156 Key-Format: `sdb_ak_<32 random base62>`. key_prefix = erste 8 Zeichen für Logs.

---

## Config & Deployment

- [x] #126 `AddSproutDB(options => ...)` DI Extension – SproutEngineSettingsBuilder + SproutServiceCollectionExtensions. Registriert SproutEngine + ISproutServer als Singleton.
- [ ] #128 appsettings.json Konfiguration: DataDirectory, DefaultPageSize, WalFlushIntervalSeconds, PreAllocateChunkSize, AutoIndex Settings, Auth Settings.
- [x] #145 Pre-Allocation Chunk Size konfigurierbar: `SproutEngineSettings.ChunkSize`. Durchgereicht durch TableCache → TableHandle → ColumnHandle/IndexHandle → CreateTableExecutor.

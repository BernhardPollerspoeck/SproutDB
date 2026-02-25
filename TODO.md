# SproutDB – Offene Punkte

Jeder Punkt ist ein Feature, Syntax-Element, Konzept oder Anforderung aus den Design-Dokumenten der noch nicht implementiert ist.

---

## Upsert

- [ ] `upsert users {email: 'john@test.com', name: 'John Doe'} on email` – Upsert mit Match-Spalte (ON clause). Existiert ein Record mit diesem Wert? → Update. Sonst → Insert.
- [ ] `upsert users {id: 42, email: null}` – Explizites NULL-Setzen per Upsert. Nur wenn Spalte nullable ist, sonst Fehler NOT_NULLABLE.
- [ ] Upsert Response muss den **kompletten Record** im finalen State zurückgeben – inkl. aller bestehenden Felder die nicht geändert wurden. Kein extra GET nötig.
- [ ] Upsert Response: `operation` Sub-Feld als `"insert"` oder `"update"` (bei Bulk eine Liste). Aktuell nur Byte-Enum 2 (upsert) ohne Unterscheidung insert/update.
- [ ] Bulk Upsert: `upsert users [{name: 'John', age: 25}, {name: 'Jane', age: 30}]` – Array-Syntax mit mehreren Records.
- [ ] Bulk Upsert mit ON: `upsert users [{...}, {...}] on email` – Bulk + Match-Spalte kombiniert.
- [ ] Bulk Upsert Response: alle betroffenen Records zurückgeben.
- [ ] Bulk Limit: konfigurierbares Limit für Bulk Upsert, Error Code `BULK_LIMIT` wenn überschritten.

---

- [ ] `get users -select age, email` – Exclude-Select. Gibt alle Spalten zurück AUSSER den angegebenen. Entweder `select` oder `-select`, nie beides (Mischen → Fehler). `-select id` ist erlaubt.
- [ ] `get users where age > 30` – Where-Clause mit Vergleichsoperator `>`.
- [ ] `get users where name = 'John'` – Where-Clause mit Gleichheit `=`.
- [ ] `get users where age >= 18` – Where-Clause mit `>=`.
- [ ] `get users where age <= 30` – Where-Clause mit `<=`.
- [ ] `get users where age < 18` – Where-Clause mit `<`.
- [ ] `get users where age != 18` – Where-Clause mit Ungleichheit (falls supported, aus Samples ableitbar).
- [ ] `get users where email contains '@gmail'` – String-Operator `contains`.
- [ ] `get users where name starts 'Jo'` – String-Operator `starts`.
- [ ] `get users where name ends 'son'` – String-Operator `ends`.
- [ ] `get users where age >= 18 and active = true` – Logischer Operator `and`.
- [ ] `get users where role = 'admin' or role = 'moderator'` – Logischer Operator `or`.
- [ ] `get users where not active = true` – Negation mit `not`.
- [ ] `get users where role in ['admin', 'moderator']` – `in`-Operator mit Array-Literal.
- [ ] `get users where role not in ['banned', 'suspended']` – `not in`-Operator.
- [ ] `get users where age between 18 and 30` – `between`-Operator (inklusiv, wie SQL).
- [ ] `get users where age not between 18 and 30` – `not between`.
- [ ] `get users where email is null` – NULL-Check mit `is null`.
- [ ] `get users where email is not null` – NULL-Check mit `is not null`.
- [ ] Where mit Datum/Zeit-Vergleich: `get users where created > '2025-01-01 14:30:00.0000'`
- [ ] Where mit Date-Vergleich: `get users where birthday > '2000-01-01'`
- [ ] Where mit Time-Vergleich: `get users where shift_start > '08:00:00.0000'`
- [ ] Where mit DateTime-Between: `get users where created between '2025-01-01 00:00:00.0000' and '2025-06-01 00:00:00.0000'`
- [ ] `get users where age > 18 order by name` – ORDER BY (ascending default).
- [ ] `get users where age > 18 order by age desc` – ORDER BY DESC.
- [ ] `get users where active = true order by created desc, name` – Multi-Column ORDER BY.
- [ ] `get users where active = true limit 10` – LIMIT.
- [ ] `get users order by created desc limit 5` – LIMIT ohne WHERE.
- [ ] `get users count` – COUNT (Anzahl Rows).
- [ ] `get users where active = true count` – COUNT mit WHERE.
- [ ] `get users select city distinct` – DISTINCT.
- [ ] `get users select role distinct where active = true` – DISTINCT mit WHERE.
- [ ] `get orders sum amount` – Aggregation SUM.
- [ ] `get orders avg amount` – Aggregation AVG.
- [ ] `get orders min amount` – Aggregation MIN.
- [ ] `get orders max amount` – Aggregation MAX.
- [ ] `get orders sum amount as total_revenue` – Aggregation mit Alias (AS).
- [ ] `get orders avg amount as average_order_value where status = 'completed'` – Aggregation + Alias + WHERE.
- [ ] `get orders sum amount where status = 'completed'` – Aggregation mit WHERE.
- [ ] `get orders sum amount as revenue group by status` – GROUP BY.
- [ ] `get orders count group by city` – COUNT mit GROUP BY.
- [ ] `get orders avg amount as avg_amount group by customer_id order by avg_amount desc limit 10` – GROUP BY + ORDER BY + LIMIT kombiniert.
- [ ] `get orders select amount, amount * 0.19 as tax` – Computed Fields mit Arithmetik und Alias.
- [ ] `get orders select name, price, quantity, price * quantity as total` – Computed Fields mit mehreren Spalten.
- [ ] Computed Fields Typ-Inferenz: immer der breiteste Typ (`sint * double` → `double`, `ubyte + uint` → `uint`).
- [ ] `get users follow users.id -> orders.user_id as orders` – JOIN (follow Syntax).
- [ ] Gefilterter Join: `get users where active = true follow users.id -> orders.user_id as orders where orders.status = 'completed'`
- [ ] Mehrere Joins: `get users follow users.id -> orders.user_id as orders follow orders.product_id -> products.id as product`
- [ ] `delete users where id = 42` – DELETE einzelner Record.
- [ ] `delete users where active = false` – DELETE mit Bedingung.
- [ ] `delete users where last_login < '2024-01-01'` – DELETE mit Datum-Bedingung.
- [ ] Delete Response: returnt die gelöschten Records (komplette Records für Logging/Undo/Auditing).
- [ ] Delete Storage: Alle .col Files an der Place-Position nullen (Flag 0x00 + Null-Bytes), _index ID-Position auf 0 setzen.
- [ ] Free-List / Place-Reuse bei Insert: Scan _index für ersten Eintrag mit Wert 0 → freier Place wiederverwenden statt immer appenden. (Design-Entscheidung: Stack LIFO oder PriorityQueue min-heap)
- [ ] `rename column users.old_name to new_name` – RENAME COLUMN. Benennt .col File um + Schema update.
- [ ] `alter column users.bio string 10000` – ALTER COLUMN (String-Länge ändern). Rebuilds .col File mit neuer Entry Size, Pointer-Swap, Reads laufen währenddessen weiter. Vergrößern und Verkleinern erlaubt (Daten die nicht passen werden abgeschnitten).
- [ ] `purge column users.old_field` – PURGE COLUMN. Löscht .col File + Schema update. Endgültig, kein Undo.
- [ ] `purge table users` – PURGE TABLE. Löscht Table-Verzeichnis komplett. Endgültig.
- [ ] `purge database` – PURGE DATABASE. Löscht Database-Verzeichnis aus dem Header. Endgültig.
- [ ] `describe users` – DESCRIBE TABLE. Returnt alle Spalten mit Typ, nullable, default, strict, auto. Response enthält `schema.table` und `schema.columns` Array.
- [ ] `describe` (ohne Argument) – DESCRIBE ALL. Returnt alle Tabellen der aktuellen Datenbank als `schema.tables` Array.
- [ ] Auto-Paging: Wenn GET-Result die Page Size überschreitet, kommt automatisch Paging. Response enthält `paging.total`, `paging.page_size`, `paging.page`, `paging.next` (fertige Query für nächste Seite).
- [ ] `get users where active = true page 2 size 100` – Manuelle Paging-Syntax mit `page` und `size` Keywords.
- [ ] Default Page Size / Bulk Limit: 100 (ein Setting für beides). Konfigurierbar auf Server-Ebene, Override pro Database möglich.
- [ ] Kommentare in Queries: `##` startet UND beendet einen Kommentar (inline toggle). Kommentare am Zeilenende brauchen kein schließendes `##`.
- [ ] `_system` Database: System-Database, komplett read-only für alle User. Nur SproutDB intern schreibt.
- [ ] Audit Log: Alle Schema-Änderungen werden in `_system.audit_log` geloggt. Felder: timestamp, user, database, query, operation. NICHT geloggt: Daten-Operationen (get, upsert, delete).
- [ ] Audit Log abfragbar: `get audit_log` mit Header `X-SproutDB-Database: _system`.
- [ ] HTTP Server: `POST /query` Endpoint. Content-Type: text/plain. Query als Plain Text im Body, Response immer JSON.
- [ ] HTTP Header `X-SproutDB-Database` (Pflicht) – Aktive Datenbank.
- [ ] HTTP Header `X-SproutDB-ApiKey` (Pflicht wenn Auth aktiv) – API Key für Authentifizierung.
- [ ] HTTP Status Code Mapping: 200 OK, 400 Query-Fehler, 401 Auth fehlend, 403 Keine Berechtigung, 404 Not Found, 409 Conflict.
- [ ] SproutDB.Server Projekt: ASP.NET Host mit Kestrel.
- [ ] `MapSproutDB()` Extension Method – registriert POST /query Endpoint.
- [ ] `MapSproutDBHub()` Extension Method – registriert SignalR Hub auf `/sproutdb/changes`.
- [ ] `AddSproutDB(options => ...)` DI Extension – registriert Engine im DI Container mit Settings.
- [ ] `AddSproutDBAuth(options => ...)` DI Extension – aktiviert Auth.
- [ ] appsettings.json Konfiguration: DataDirectory, DefaultPageSize, WalFlushIntervalSeconds, PreAllocateChunkSize, AutoIndex Settings, Auth Settings.
- [ ] Docker Compose Support.
- [ ] SignalR Hub: `/sproutdb/changes` Endpoint. Auth via `X-SproutDB-ApiKey` per Query String oder Header.
- [ ] SignalR: `Subscribe(string database, string table)` – Client tritt Group bei, empfängt Changes.
- [ ] SignalR: `Unsubscribe(string database, string table)` – Client verlässt Group.
- [ ] SignalR: `OnChange(SproutResponse event)` – Server → Client Push. Selbes Response-Objekt wie HTTP.
- [ ] SignalR Groups: Data-Changes auf `{database}.{table}`, Schema-Changes auf `{database}._schema`.
- [ ] SignalR: Kein Listener in Group → kein Overhead. Bei Disconnect räumt SignalR Group-Membership auf.
- [ ] SignalR: Permissions greifen – nur Databases für die der API Key Zugriff hat.
- [ ] Schema Change Events an `{database}._schema` Group: `add_column`, `purge_column`, `rename_column`, `purge_table`, `create_table`.
- [ ] Permissions: 3 Rollen – `admin` (alles), `writer` (upsert, delete, get, describe), `reader` (get, describe).
- [ ] Permissions: API Key pro User (JWT Bearer als Alternative möglich). ASP.NET Middleware handelt Auth.
- [ ] Permissions: v1 auf Database-Ebene, v2 Table-Level und Column-Level.
- [ ] Error Code `AUTH_REQUIRED` – Auth fehlend.
- [ ] Error Code `AUTH_INVALID` – Auth ungültig.
- [ ] Error Code `PERMISSION_DENIED` – Rolle hat keine Berechtigung.
- [ ] ISproutServer Interface: `GetOrCreateDatabase()`, `SelectDatabase()`, `GetDatabases()`, `Migrate()`.
- [ ] ISproutDatabase Interface: `Query()` für Query-String API.
- [ ] IMigration Interface: `int Order { get; }` und `void Up(ISproutDatabase db)`. Migration weiß nicht gegen welche Database sie läuft.
- [ ] Migration Tracking: `_migrations` Table pro Database (read-only für User). Felder: name, order, executed timestamp.
- [ ] `sprout.Migrate(assembly, database)` – Scannt Assembly für IMigration, führt fehlende aus, schreibt in `_migrations`.
- [ ] Migrations laufen VOR dem Öffnen der HTTP/SignalR Endpoints. Fehlgeschlagene Migration → Server startet nicht.
- [ ] `db.ExportToZip("/backups/shop.zip")` – Database = Verzeichnis. ZIP = vollständiges Backup inkl. Daten, Schema, Migrations-History.
- [ ] `sprout.ImportFromZip("/backups/shop.zip", "shop_restored")` – Unzip in neues Verzeichnis, MMFs öffnen, fertig.
- [ ] In-Process Change Notifications: `users.OnChange(change => { ... })` – Selbes Event wie SignalR, nur als Callback ohne Serialisierung.
- [ ] Annotated Query: Fehler-Kommentare (`##...##`) an der **exakten Fehlerstelle inline** einfügen statt nur als Suffix. Z.B. `get users where agee ##unknown column 'agee'## > 18`. Aktuell nur Suffix-Annotation.
- [ ] Mehrere Fehler sammeln: Bei nicht-Dead-Stop Fehlern (z.B. mehrere unbekannte Spalten) alle Fehler sammeln und gemeinsam zurückgeben statt beim ersten abzubrechen.
- [ ] Pre-Allocation Chunk Size konfigurierbar machen. Aktuell hardcoded 10.000 in `StorageConstants`. Soll über `SproutEngineSettings.PreAllocateChunkSize` steuerbar sein.
- [ ] `get orders avg amount where created > '2025-01-01 00:00:00.0000'` – Aggregation + WHERE + Datums-Vergleich kombiniert.

---

## Auto-Index

- [ ] `create index users.email` – Manueller Index erstellen. B-Tree File (.btree) wird geschrieben.
- [ ] `purge index users.email` – Manueller Index löschen.
- [ ] B-Tree Index File-Format (.btree): mappt Column-Wert → Place. Kompakt, nur indexierter Wert + Place (Long).
- [ ] Query Engine: hat die Spalte ein .btree? → B-Tree Lookup. Kein .btree? → .col File Scan.
- [ ] B-Tree Update bei Writes: Bei jedem Insert/Update/Delete prüfen ob .btree existiert für betroffene Spalten → B-Tree Update (O(log n)).
- [ ] Auto-Index: Usage Frequency Tracking – Counter wie oft Spalte in Where-Clauses genutzt wird, relativ zur Gesamtzahl Queries auf die Table.
- [ ] Auto-Index: Selektivität Tracking – Verhältnis Rows gescannt vs. Rows im Result.
- [ ] Auto-Index: Read/Write Ratio Tracking – Verhältnis Reads vs. Writes auf die Table.
- [ ] Auto-Index: Entscheidungslogik – Hohe Nutzung + Hohe Selektivität + Read-Heavy → Index anlegen. Sonst → kein Index.
- [ ] Auto-Index: Konfigurierbare Schwellwerte – Usage >30%, Selektivität >95% Verwurf, Read-Heavy >3:1 R/W Ratio. Alle Defaults konfigurierbar pro Server.
- [ ] Auto-Index: Metriken in `_system.index_metrics` Table persistieren. Abfragbar mit normalen GET Queries.
- [ ] Auto-Index: Index-Build in der Single-Writer Queue (sequentieller Scan über .col File → B-Tree aufbauen → .btree schreiben → Pointer-Swap).
- [ ] Auto-Index: Ungenutzte Auto-Indizes nach 30 Tagen entfernen (konfigurierbar). Manuell erstellte Indizes werden nicht angefasst.
- [ ] Auto-Index: Logging warum ein Index erstellt oder entfernt wurde.

---

## Fluent API

- [ ] Fluent API: `db.CreateTable("users").AddColumn<string>("name").AddColumn<int>("age")` etc.
- [ ] Fluent API: `db.AddColumn<bool>("users", "premium", defaultValue: false)`.
- [ ] Fluent API: `db.AlterColumn("users", "bio", size: 10000)`.

---

## LINQ API

- [ ] Typed LINQ API: `db.Table<T>("users")` – Table-Accessor mit generischem Typ.
- [ ] LINQ: `.Where(u => u.Age > 18)` – Expression Tree → internes Query-Objekt.
- [ ] LINQ: `.Select(u => u.Name)` – Projection.
- [ ] LINQ: `.FirstOrDefault(u => u.Id == 42)` – Single Record.
- [ ] LINQ: `.Count()` – Zählen.
- [ ] LINQ: `.OrderBy()` / `.OrderByDescending()` – Sortierung.
- [ ] LINQ: `.Take(10)` – Limit.
- [ ] LINQ: `.Run()` – Returns SproutResponse (same as HTTP/Query String).
- [ ] LINQ: `.ToList()` – Typed Results.
- [ ] LINQ: `users.Upsert(new User { ... })` – Typed Insert/Update.
- [ ] LINQ: `users.Upsert(new { Id = 1ul, Age = (byte)26 })` – Partial Update mit anonymem Objekt.
- [ ] LINQ: `users.Upsert(new User { ... }, on: u => u.Email)` – Upsert mit Match-Column.
- [ ] LINQ: `users.Upsert(new[] { user1, user2 }, on: u => u.Email)` – Bulk Upsert.
- [ ] LINQ: `users.Delete(u => u.Active == false)` – Typed Delete.

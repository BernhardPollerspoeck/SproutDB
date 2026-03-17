# SproutDB — TODO

## Offen

### Alias (~6-8h) — Designed
Gespeicherte Query-Fragmente mit einem Namen. Auflösung auf AST-Ebene.

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

### Full-Text Search (~12-16h) — Design fehlt
Textsuche über String-Spalten (`where body search 'machine learning'`). Offene Fragen: Index-Typ, Tokenizer/Stemming, Fuzzy, Sprach-Support.

### JSON Column Type (~10-14h) — Design fehlt
Für semi-strukturierte Daten (Konfiguration, Metadaten).

### Admin UI
- [ ] Auto-Index Status auf Monitoring Page — Aktive Indizes mit auto/manual Badge
- [ ] Auto-Index Suggestions — Spalten die Schwellenwerte fast erreichen

### Autocomplete (~8-10h) — ~60% fertig
Umfangreiche Test-Matrix mit ~70 Szenarien. Fehlende Bereiche:
- [ ] `and`/`or` nach WHERE-Bedingung → Column-Suggestions
- [ ] Komma-getrennte Spalten (`select name, ⎸`) → Column-Suggestions
- [ ] DELETE Autocomplete (table nach `delete`, dann `where`)
- [ ] DESCRIBE Autocomplete (table nach `describe`)
- [ ] CREATE sub-argument Completion (z.B. `create table ⎸` → table names)
- [ ] PURGE Argument-Completion (z.B. `purge table ⎸` → table names)
- [ ] Auth-Befehle Completion (`grant`, `revoke`, `restrict`, `unrestrict`)
- [ ] Follow-Clause Completion (`follow ⎸` → table.column Suggestions)
- [ ] `atomic` / `commit` Context (nach `atomic;` → nur write commands + `commit`)

### Bekannte Einschränkungen
- **Numerische Typen großzügig wählen** — Type Widening (`ubyte` → `ushort` etc.) funktioniert via `add column table.col newtype`, aber erfordert File-Rebuild.

---

## Implementiert

| Feature | Details |
|---------|---------|
| Multi-Query Batching | Semicolons als Delimiter, `Execute()` → `List<SproutResponse>` |
| Transactions | `atomic; ...; commit`, TransactionJournal mit MMF-Rollback, read-your-own-writes |
| Type Widening | Funktioniert (10 Tests grün) |
| Auto-Index | B-Tree, Metrics, Evaluator, auto create/purge |
| Auth | API Keys, Rollen pro DB, Restrict/Unrestrict pro Table |
| HTTP API | `POST /sproutdb/query`, immer JSON Array Response |
| SignalR | Change Notifications via Hub |
| Admin UI | Query Editor, Schema Browser, Monitoring, Auth Management |
| Migrations | `IMigration`, Once/OnStartup, `MigrationRunner` |
| Joins/Follow | Inner/Left/Right/Outer via Arrow-Syntax, Pre-Filter per Follow |
| Backup/Restore | ZIP-basiert |
| Shrink | Table + Database Level, Chunk-Size Anpassung |
| TTL | Table-Level + Row-Level, Background Cleanup |
| Paging | `page N size M`, cursor-basiert |
| Computed Columns | `select price * quantity as total` |
| Aggregation | `sum`, `avg`, `min`, `max`, `count`, `group by` |
| Unique Indexes | `create unique index table.col` |
| Blob/Array Columns | Base64 Blobs, JSON Arrays |

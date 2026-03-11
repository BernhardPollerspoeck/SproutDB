# SproutDB v2 – Feature Planning

**Stand:** 2026-03-11
**Status:** Living Document

---

## Designed – bereit zur Implementierung

### Alias ~6-8h

Gespeicherte Query-Fragmente mit einem Namen. Auflösung auf AST-Ebene (nicht textuell). Engine weiß nichts von Aliases — Auflösung passiert im Pre-Processor vor der Engine.

```
create alias active_users as get users where active = true
purge alias active_users
```

**Aufruf-Syntax:** `load` als Keyword, immer Startpunkt der Query:
```
load active_users where age > 25
load active_users
    follow users.id -> orders.user_id as orders
    select name, orders.total
```

**Regeln:**
- `create alias` überschreibt bei bestehendem Namen
- `purge alias` löscht
- Nur `get`-Queries als Alias-Body erlaubt
- Validierung bei Erstellung (Table/Spalten müssen existieren)
- Scope: pro Database, in der DB persistiert
- Verschachtelung erlaubt (Alias referenziert Alias), Circle Detection bei Auflösung
- Kombination mit `where`, `follow`, `select`, `distinct` am Aufruf erlaubt
- Paging (`page`/`limit`) darf NICHT im Alias-Body stehen (wird am Aufruf gesteuert)

**Execution — Nested (kein AST-Merge):**
- `load` führt den Alias komplett aus → materialisierte Rows (`List<Dictionary<string, object?>>`)
- Aufrufer-Query (where, follow, select, distinct, order, page) operiert auf diesem Ergebnis
- GetExecutor bekommt zweiten Eingang: statt `ReadRows()` fertige Row-Liste als Input
- Kein Merge-Problem — zwei getrennte Execution-Stufen
- Fehlende Spalten (z.B. Follow-Key nicht im Alias-Select) → Fehler, User-Verantwortung

**Storage:**
- `_aliases.idx` — Index-File: Name (max 50 Zeichen) + Offset pro Alias
- `_aliases.dat` — Data-File: Query-Texte `\0`-terminiert an den Offsets aus dem Index
- Lücken (gelöschte Aliases, Offset = -1): beim Erstellen prüfen ob neuer Alias in Lücke passt → File wächst nicht unnötig
- O(1) Lookup über Index

### Transactions (atomic/commit) ~6-8h

Atomarer Batch im Single Writer. Semikolon als Delimiter, atomic/commit als Block-Marker.

```
atomic;
upsert accounts set balance = 900 where id = 1;
upsert accounts set balance = 1100 where id = 2;
commit
```

**Regeln:**
- `commit` = abschließen und schreiben
- Fehler bei einer Op → sofort Rollback der gesamten Gruppe
- WAL schreibt alle Ops mit Group-ID, bei Crash vor `commit` wird die Gruppe beim Recovery verworfen
- Nur Write-Ops (upsert/delete) erlaubt, kein GET innerhalb Transaction
- Keine Verschachtelung (`atomic` in `atomic` → Fehler)
- Cross-Table erlaubt
- Keine Limit für Anzahl Ops
- Muss als eine Query geschickt werden (`atomic` ohne `commit` → Parser-Fehler)

Kein Ersatz für Application-Level Transaction-Logik bei externen API Calls – das ist nicht Job der DB.

### Distinct ~1-2h

Eigenes Keyword, kein Teil von select. Wirkt als Post-Filter nach Where, vor Project.

```
get users distinct name
get users distinct name, email
get users where age > 25 distinct city select name, city
```

Pipeline-Reihenfolge: Filter → Distinct → Project → Auto-Page.

### Unique Constraint ~2-3h

Über Index gelöst. `create index unique` erzeugt Index + Constraint in einem.

```
create index unique users.email
create index unique users.email, users.tenant_id
```

**Regeln:**
- Single-Column und Composite Unique
- Prüfung bei jedem Insert/Upsert via B-Tree Lookup (O(log n)). Kein Table Scan.
- Null-Werte: Mehrere Nulls erlaubt (null = unbekannt, kein Unique-Verstoß)
- Unique-Violation → eigener Error-Typ
- `create index unique` auf Spalte mit bestehenden Duplikaten → Fehler

### Blob/File Storage ~4-5h

Eigener Spaltentyp `blob`. Storage als File per Row: `columnname_rowid.blob` im Table-Verzeichnis. Die Daten liegen als Rohbytes im jeweiligen File.

```
add column mytable avatar blob
```

**Datentyp:** Wird bei `add column` mit `blob` deklariert. Engine weiß dann: Wert als eigenes File speichern statt in die Column-File.

**Encoding:** Base64 rein und raus. Wert im Query-String ist Base64, Wert im JSON-Response ist Base64. Kein separater Binary-Endpoint.

**Operationen:**
- **Write/Insert**: Base64-String → Raw-Bytes → File schreiben
- **Read**: Raw-Bytes → Base64 → im JSON-Response
- **Update**: Rename old File → Write new File → Delete renamed (Crash-Safe)
- **Delete Row**: File direkt löschen
- **Where**: Erlaubt (vergleicht Base64-Strings, kein Sonderverhalten)
- **Select**: Blob-Spalten werden wie normale Spalten behandelt, kein Sonderverhalten

**Storage:** Passt zum Column-per-File Prinzip. Jede Row ein eigenes File – keine Größenbeschränkung, kein Chunking. Backup (ZIP) packt Blob-Files implizit mit ein (ist im DB-Folder).

---

## Geplant – Design ausstehend

### Full-Text Search

Textsuche über String-Spalten. Mögliche Syntax:

```
get users where name contains 'berg'
get articles where body search 'machine learning'
```

Offene Fragen: eigener Index-Typ? Tokenizer/Stemming? Nur exakter Match oder Fuzzy? Sprach-Support?

---

## Architektur-Prinzipien

- Single NuGet Package, alles drin, ~348 KB
- Embedded und Networked aus demselben Package
- Explizit über implizit
- Engine bleibt dumm und schnell – Features wie Alias und Plan sind Pre/Post-Processing Layer
- Column-per-File, MMF, Single Writer, WAL
- Query Language die lesbar ist, kein SQL

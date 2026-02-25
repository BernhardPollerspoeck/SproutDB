# SproutDB – Consors Briefing Phase 1

## Kontext
SproutDB ist eine .NET-native Datenbank-Engine. Embedded und networked. Dieses Dokument ist dein Arbeitsauftrag für Phase 1.

Lies ZUERST die 5 Design-Dokumente – sie enthalten alle Architektur-Entscheidungen:
1. `sproutdb-query-design.md` – Query Language Spec
2. `sproutdb-persistence-design.md` – Column-per-File Storage, MMF, WAL
3. `sproutdb-inprocess-design.md` – In-Process API, Migrations
4. `sproutdb-api-reference.md` – HTTP/SignalR API, Response Format, Status Codes
5. `sproutdb-autoindex-design.md` – Auto-Index (NICHT in Phase 1, nur lesen für Kontext)

---

## Target Framework
- .NET 9
- C# latest

---

## Solution-Struktur

```
SproutDB/
  src/
    SproutDB.Core/              ## Engine: Parser, Storage, Query Execution
    SproutDB.Server/            ## ASP.NET Host: Kestrel, HTTP Endpoint, SignalR (NICHT Phase 1)
  tests/
    SproutDB.Core.Tests/        ## xUnit Unit Tests
  benchmarks/
    SproutDB.Benchmarks/        ## BenchmarkDotNet
```

Phase 1 ist nur `SproutDB.Core`, `SproutDB.Core.Tests` und `SproutDB.Benchmarks`.

---

## Phase 1 Scope

### Was gebaut wird
1. **Parser** – Query-String → internes Query-Objekt
2. **Schema-Ops** – `create database`, `create table`, `add column`
3. **Basic Upsert** – `upsert` mit einzelnem Record, ID-Generierung (ulong, auto-increment)
4. **Basic Get** – `get` mit `select` (ohne `-select`, ohne `where`, ohne `order`, ohne `paging`, ohne Joins, ohne Aggregation)
5. **Persistence** – Column-per-File Storage mit Memory-Mapped Files, WAL, Flush-Zyklus
6. **Universelles Response-Format** – exakt wie in der API Reference definiert (Operation Enum, alle Felder immer vorhanden)

### Was NICHT gebaut wird (spätere Phasen)
- HTTP Server / SignalR
- `where`, `order by`, `limit`, `paging`
- `-select`, `distinct`, `count`
- `delete`
- `describe`, `rename column`, `alter column`, `purge` (table/database/column)
- Joins (`follow`)
- Aggregation (`sum`, `avg`, `min`, `max`, `group by`)
- Computed Fields
- Permissions / Auth
- Auto-Index
- In-Process LINQ API
- Migrations
- Bulk Upsert

---

## Architektur-Vorgaben

### Parser
- Input: Query-String (UTF-8)
- Output: typisiertes Query-Objekt (z.B. `CreateTableQuery`, `UpsertQuery`, `GetQuery`)
- Case-insensitive (intern alles lowercase)
- Single-Quote Strings (`'wert'`)
- Bei Fehler: annotierte Query mit `##`-Kommentaren an Fehlerstelle
- Mehrere Fehler sammeln wenn möglich, Dead-Stop bei Syntax-Fehlern
- Parser muss erweiterbar sein – neue Kommandos kommen in späteren Phasen

### Storage Engine (aus sproutdb-persistence-design.md)
- Column-per-File: jede Spalte ein eigenes `.col` File
- `_index` File: Memory-Mapped, Long pro ID, Wert = Place in .col Files, 0 = gelöscht/frei
- `.col` Files: 1 Flag-Byte (0x00=null, 0x01=Wert) + Fixed-Size Wert pro Entry
- Strings: null-terminated innerhalb Fixed-Size Block, Default 255 Bytes
- ID: auto-generiert, ulong (uint64), startet bei 1
- Insert: freien Place wiederverwenden (Index=0) oder ans Ende appenden
- Pre-Allocation: Chunks von 10.000 Places

### Verzeichnisstruktur
```
/sproutdb-data/
  _system/
  shop/
    _meta.json
    users/
      _index
      _schema.json
      name.col
      email.col
      age.col
```

### WAL
- Format: `[Sequence: int64][QueryLength: int32][Query: UTF-8 string]`
- Write-Pfad: WAL append + fsync → MMF update → Response
- Flush-Zyklus: alle X Sekunden MMFs flushen, WAL truncaten
- Crash Recovery: WAL Replay mit Sequence Numbers (idempotent)

### Concurrency
- Single-Writer Queue (`Channel<T>`)
- Reads lock-free parallel auf MMFs
- Kein Locking

### Response Format
- Immer dasselbe Objekt, alle Felder immer vorhanden
- Operation als `byte` Enum (0=error, 1=get, 2=upsert, ... siehe API Reference)
- Nicht benötigte Felder sind `null` / `0`, niemals weggelassen

---

## Tests (xUnit)

### Was getestet wird
- Parser: jede Query-Variante, Fehlerfälle mit annotierten Queries
- Storage: create database/table/column, Verzeichnisstruktur korrekt
- Upsert: Insert mit Auto-ID, partieller Update, Nullable/Default Handling
- Get: alle Spalten, select bestimmte Spalten
- WAL: Write + Replay nach simuliertem Crash
- Concurrency: parallele Reads während Write
- Edge Cases: leerer Upsert `{}`, NULL-Werte, String am Limit der Spaltenbreite

### Konventionen
- Ein Testprojekt: `SproutDB.Core.Tests`
- xUnit, kein Moq – direkt gegen die Engine testen
- Jeder Test erstellt sein eigenes temp-Verzeichnis, räumt danach auf
- Keine Shared State zwischen Tests

---

## Benchmarks (BenchmarkDotNet)

### Setup
- Eigenes Projekt: `SproutDB.Benchmarks`
- `[MemoryDiagnoser]` auf jeder Benchmark-Klasse (misst Allocations + GC)
- Ausführen mit: `dotnet run -c Release`

### Was gebenchmarkt wird
- Parser: Throughput (Queries pro Sekunde)
- Upsert: Single Record Write-Latenz und Throughput
- Get: Read-Latenz bei verschiedenen Table-Größen (100, 10.000, 1.000.000 Rows)
- MMF: Random Access Read vs Sequential Scan
- WAL: Append-Throughput
- Allocations: Zero-Alloc wo möglich bei Reads

### Konventionen
- Benchmark-Daten im `[GlobalSetup]` vorbereiten
- Realistische Datenmengen, keine Toy-Benchmarks
- Ergebnisse als Baseline speichern für Regression-Tracking

---

## Code-Qualität

- Kein `dynamic`, kein `object` wo vermeidbar
- `Span<T>` und `Memory<T>` für Binary-Operationen
- Keine unnötigen Allocations auf dem Hot Path (Read/Write)
- Interne Klassen `internal`, nur die öffentliche API ist `public`
- XML-Docs auf public API
- Keine externen Dependencies im Core außer BenchmarkDotNet (nur im Benchmark-Projekt)

### Architektur
- Single Responsibility: jede Klasse hat genau eine Aufgabe. Keine Multi-Purpose Klassen.
- Saubere Aufteilung in kleine, fokussierte Klassen und Interfaces.
- Interfaces nutzen wenn Abstraktion nötig ist – ABER: kein Interface wenn Inlining oder Devirtualisierung dadurch verhindert wird. Auf dem Hot Path (Read/Write) konkrete Typen bevorzugen damit der JIT optimieren kann.
- Composition over Inheritance.

### Iteratoren / Lazy Evaluation
- Intern IMMER `IEnumerable<T>` / `yield return` statt `ToList()` oder Array-Materialisierung.
- Kein vollständiges Materialisieren von Zwischenergebnissen. Daten fließen als Stream durch die Pipeline (Scan → Filter → Project → Response).
- `ToList()` / `ToArray()` nur am allerletzten Schritt wenn die Response serialisiert wird.
- Beispiel: ein `get users where age > 18 select name` scannt die .col Files als Iterator, filtert lazy, projiziert lazy, und materialisiert erst für die JSON-Response.

---

## Reihenfolge

1. Solution + Projekte aufsetzen
2. Response-Objekt (`SproutResponse`) + Operation Enum
3. Parser Grundgerüst mit `create database`
4. Storage: Verzeichnisse anlegen, `_schema.json` schreiben
5. Parser + Storage: `create table` mit Spalten
6. Parser + Storage: `add column`
7. Auto-Increment ID-Generator (ulong)
8. Parser + Storage: `upsert` (Insert mit Auto-ID)
9. WAL: Write + Replay
10. Parser + Storage: `get` mit `select`
11. Flush-Zyklus
12. Unit Tests parallel zu jedem Schritt
13. Benchmarks nach Schritt 10

---

## Wichtig

- Lies die Design-Dokumente. Alles was dort steht ist entschieden. Nicht neu erfinden.
- Frag bei Unklarheiten. Lieber einmal fragen als falsch bauen.
- Architektur muss skalierbar sein. Phase 1 ist minimal, aber die Struktur muss `where`, `delete`, Joins, SignalR etc. später tragen können ohne Refactor.
- Performance ist von Tag 1 wichtig. Kein "optimieren wir später". Allocations und MMF-Zugriffe von Anfang an richtig machen.

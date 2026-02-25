# SproutDB Persistence Engine Design

## Strategie
- Column-per-File Architektur
- Alle Files als Memory-Mapped Files (MMF)
- Fixed-Size Entries in jedem File
- WAL für Crash Recovery / Durability
- ID-basierter Index für Place-Lookup
- Schema-Änderungen ohne Table-Rebuild

---

## Verzeichnisstruktur

```
/sproutdb-data/
  _system/
    audit_log/
      _index
      _schema.json
      timestamp.col
      user.col
      database.col
      query.col
      operation.col
  shop/
    _meta.json
    users/
      _index              ## ID → Place (Long pro ID)
      _schema.json        ## Spalten, Typen, Größen, next_id
      name.col            ## Flag-Byte + 255 Bytes pro Place
      age.col             ## Flag-Byte + 1 Byte pro Place (ubyte)
      active.col          ## Flag-Byte + 1 Byte pro Place
      email.col
    orders/
      _index
      _schema.json
      ...
```

> Database = Verzeichnis. Table = Unterverzeichnis.
> Jede Column = eigenes .col File.
> `_index` und `_schema.json` pro Table.
> `_meta.json` pro Database (Konfiguration wie Page Size Override).

---

## Index File (_index)

Memory-Mapped File mit Longs (8 Bytes pro Eintrag).

```
Position = ID (startet bei 1, Position 0 ist ungenutzt)
Wert     = Place in den .col Files
Wert 0   = gelöscht / frei

_index:
Pos 0: [---]         ## ungenutzt (ID startet bei 1)
Pos 1: [Place 0]     ## ID 1 → Place 0
Pos 2: [Place 1]     ## ID 2 → Place 1
Pos 3: [0]           ## ID 3 → gelöscht
Pos 4: [Place 2]     ## ID 4 → Place 2
Pos 5: [Place 3]     ## ID 5 → Place 3
```

> Direkter Zugriff: ID × 8 Bytes = Offset im File.
> Scan für freie IDs: erste 0 finden.

---

## Column Files (.col)

Jeder Eintrag: 1 Flag-Byte + Fixed-Size Wert.

### Flag-Byte
```
0x00 = null
0x01 = hat Wert
(weitere Flags in Zukunft möglich)
```

### Layout Beispiel (age.col, Typ ubyte = 1 Byte)

```
Entry Size: 2 Bytes (1 Flag + 1 Wert)

Place 0: [01] [19]                        → Flag: hat Wert, age: 25
Place 1: [00] [00]                        → Flag: null
Place 2: [01] [1E]                        → Flag: hat Wert, age: 30
```

> Direkter Zugriff: Place × Entry Size = Offset im File.
> Entry Size = 1 + Column Size (aus Schema).

### Layout Beispiel (name.col, Typ string 255)

```
Entry Size: 256 Bytes (1 Flag + 255 Wert)

Place 0: [01] [4A 6F 68 6E 00 00 ...]    → Flag: hat Wert, name: 'John'
Place 1: [01] [4A 61 6E 65 00 00 ...]    → Flag: hat Wert, name: 'Jane'
Place 2: [00] [00 00 00 00 00 00 ...]    → Flag: null (gelöscht/leer)
```

> Strings sind null-terminated innerhalb des Fixed-Size Blocks.

---

## Operationen

### Insert
1. Scan _index für ersten Eintrag mit Wert 0 → freier Place
2. Kein freier Place? → ans Ende aller .col Files appenden, neuer Place
3. Werte in alle .col Files an Place schreiben (Flag 0x01 + Wert, oder Flag 0x00 für null)
4. _index: nächste freie ID → Place eintragen
5. next_id in Schema hochzählen

### Read (Get)
1. Für jede relevante Row: ID aus _index → Place
2. Place × Entry Size in jedem benötigten .col File → Wert lesen
3. Flag-Byte prüfen: 0x00 = null, 0x01 = Wert nutzen
4. Nur die Columns lesen die der Query braucht (`select name, email` → nur name.col + email.col)

### Update (Upsert mit existierender ID)
1. ID in _index → Place
2. Geänderte Werte in die entsprechenden .col Files an Place schreiben
3. Unveränderte .col Files nicht anfassen

### Delete
1. ID in _index → Place
2. Alle .col Files: Place-Position nullen (Flag 0x00 + Null-Bytes)
3. _index: ID-Position auf 0 setzen

### Add Column
1. Neues .col File anlegen
2. Für jeden existierenden Place: Flag 0x00 + Null-Bytes (oder Default-Wert mit Flag 0x01)
3. Schema updaten
4. Fertig – kein Rebuild anderer Files

### Purge Column
1. .col File löschen
2. Schema updaten
3. Fertig

---

## Read-Optimierung: Select

```
get users select name, email where age > 18
```

1. `age.col` scannen → Places finden wo Flag = 0x01 und Wert > 18
2. Nur für diese Places: `name.col` und `email.col` lesen
3. Andere .col Files werden nie angefasst

> Column-per-File = nur die Daten lesen die der Query braucht. Bei 20 Spalten und `select name` liest du 1/20 der Daten.

---

## Schema File (_schema.json)

```json
{
  "table": "users",
  "created": "2026-02-24 14:30:00.0000",
  "next_id": 4728,
  "columns": [
    {"name": "name", "type": "string", "size": 255, "entry_size": 256, "nullable": true, "default": null, "strict": false},
    {"name": "email", "type": "string", "size": 320, "entry_size": 321, "nullable": true, "default": null, "strict": true},
    {"name": "age", "type": "ubyte", "size": 1, "entry_size": 2, "nullable": true, "default": null, "strict": false},
    {"name": "active", "type": "bool", "size": 1, "entry_size": 2, "nullable": false, "default": true, "strict": false}
  ]
}
```

> `entry_size` = size + 1 (Flag-Byte). Für schnelle Offset-Berechnung.
> `next_id` = nächste zu vergebende ID.

---

---

## WAL (Write-Ahead Log)

### Format
Jeder WAL Entry ist der originale Query-String:
```
upsert users {name: 'John', age: 25}
delete users where id = 42
```
> Human-readable, format-stabil über Engine-Updates hinweg.
> Parser wird beim Replay benötigt – Startup-Performance ist akzeptabel.
> Jeder Entry hat eine Sequence Number für idempotentes Replay.

### WAL Entry Struktur
```
[Sequence: int64][QueryLength: int32][Query: UTF-8 string]
```

### Write-Pfad
1. WAL Entry schreiben + fsync (Durability garantiert)
2. MMF updaten (im Page Cache)
3. Response an Client
4. SignalR Event an Group

### Flush-Zyklus (Background Timer)
- Alle X Sekunden (z.B. 5) oder alle Y Writes (z.B. 1000):
  1. `Flush()` auf alle MMFs erzwingen → Daten auf Disk
  2. WAL truncaten → leer
- WAL bleibt dadurch immer kurz (wenige Sekunden an Entries)

### Crash Recovery
- Crash zwischen MMF Update und Flush: MMF-Änderung ist nur im Page Cache, nicht auf Disk
- Beim nächsten Startup: WAL replaying stellt alle nicht-geflushten Änderungen wieder her
- Sequence Number verhindert doppeltes Apply: bereits applied → skip

### Startup-Reihenfolge
1. Schema lesen
2. Alle MMFs öffnen
3. WAL replaying (idempotent durch Sequence Numbers)
4. WAL leeren
5. Ready

---

## Concurrency

### Single-Writer Queue
- Alle Writes gehen in eine `Channel<T>` Queue
- Ein einzelner Writer-Thread arbeitet Entries ab
- Reads laufen parallel, komplett lock-free
- Kein Locking, kein Deadlock, kein Complexity

### Read/Write Isolation
- Reads arbeiten direkt auf den MMFs
- Writes ändern MMF in-place (atomare Werte bei Fixed-Size)
- Kein Dirty-Read Problem: Writes auf aligned Werte (1/2/4/8 Bytes) sind atomar auf 64-bit Systemen

---

## File-Wachstum

### Pre-Allocation in Chunks
- .col Files und _index wachsen in Chunks von 10.000 Places
- Wenn File voll: vergrößern + MMF remappen (neuer MemoryMappedViewAccessor)
- Neue Places sind genullt = frei
- Remapping ist kurzer Moment, passiert selten (alle 10.000 Inserts)

---

## Alter Column (String Length Change)

### Ablauf
1. `alter column` kommt als Write-Operation in die Queue
2. Neues .col File mit neuer Entry Size schreiben, alle Entries kopieren
3. Neues MMF öffnen
4. Pointer-Swap: volatile Referenz auf neuen MemoryMappedViewAccessor setzen
5. Neue Reads gehen sofort aufs neue File
6. Laufende Reads nutzen noch den alten Accessor (GC räumt auf)
7. Altes .col File löschen

> Writes blockieren während des Rebuilds (Single-Writer Queue).
> Reads laufen durchgehend weiter auf dem alten File bis zum Swap.

---

## Ideen

### B-Tree Index pro Column

Ein optionaler B-Tree Index pro Spalte als eigenes File:

```
users/
  _index
  _schema.json
  name.col
  email.col
  age.col
  email.btree        ← Index: email-Wert → Place
  age.btree          ← Index: age-Wert → Place
```

- B-Tree mappt `Column-Wert → Place`
- Kompakt: nur der indexierte Wert + Place (Long), keine ganzen Rows
- Fixed Column Size = gleichmäßige B-Tree Nodes
- Query Engine: hat die Spalte ein .btree? → B-Tree Lookup. Keinen? → .col File Scan.
- Passt zum Auto-Index Feature aus SproutDB: Engine erkennt häufig gefilterte Spalten → baut .btree im Hintergrund

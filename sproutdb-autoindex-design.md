# SproutDB Auto-Index Design

## Konzept
- SproutDB trackt automatisch welche Spalten in Where-Clauses genutzt werden
- Basierend auf Nutzungshäufigkeit, Selektivität und Read/Write Ratio entscheidet die Engine ob ein B-Tree Index sinnvoll ist
- Keine absoluten ms-Schwellwerte, keine Maschinenabhängigkeit
- Index-Build läuft in der Single-Writer Queue (kein Concurrency-Problem)
- Ungenutzte Indizes werden nach 30 Tagen automatisch entfernt (konfigurierbar)
- Manuell erstellte Indizes werden vom Auto-System nicht angefasst

---

## Metriken (pro Spalte pro Table)

### Nutzungshäufigkeit
- Counter: wie oft wird diese Spalte in Where-Clauses verwendet
- Relativ zur Gesamtzahl der Queries auf die Table

### Selektivität
- Verhältnis: Rows gescannt vs. Rows im Result
- Hohe Selektivität = viel scannen, wenig returnen → Index lohnt sich
- Niedrige Selektivität = Großteil der Rows matcht → Index bringt nichts

### Read/Write Ratio
- Verhältnis: Reads vs. Writes auf die Table
- Read-Heavy → Index lohnt sich (beschleunigt Reads)
- Write-Heavy → Index kostet mehr als er bringt (B-Tree Update bei jedem Write)

---

## Entscheidungslogik

```
Hohe Nutzung + Hohe Selektivität + Read-Heavy  → Index anlegen
Hohe Nutzung + Hohe Selektivität + Write-Heavy → kein Index
Hohe Nutzung + Niedrige Selektivität           → kein Index
Niedrige Nutzung                                → kein Index
```

### Default-Schwellwerte (konfigurierbar)

| Metrik | Schwellwert | Beschreibung |
|---|---|---|
| Nutzungshäufigkeit hoch | >30% der Queries | Spalte taucht in mehr als 30% der Table-Queries in Where auf |
| Nutzungshäufigkeit niedrig | <5% der Queries | Zu selten genutzt, kein Index |
| Selektivität hoch | >95% Verwurf | Weniger als 5% der Rows im Result |
| Selektivität niedrig | <50% Verwurf | Scan ist fast gleich schnell wie Index |
| Read-Heavy | >3:1 Read/Write | Mehr als 3 Reads pro Write |
| Index-Entfernung | 30 Tage ungenutzt | Auto-Index wird nach 30 Tagen ohne Nutzung entfernt |

> Defaults sind Startpunkte. Real-World Daten werden zeigen was optimal ist.
> Alle Schwellwerte sind konfigurierbar pro Server.

---

## Tracking

### Speicherort
- Metriken werden im RAM gehalten (Counter, Running Averages)
- Periodisch in `_system` Database persistiert (Table `index_metrics`)
- Abfragbar: `get index_metrics where table = 'users'` (mit Header `X-SDB-Database: _system`)

### Transparenz
- Engine loggt warum ein Index erstellt oder entfernt wurde
- User kann Entscheidungen nachvollziehen und Schwellwerte tunen

---

## Index-Erstellung (Auto)

### Ablauf
1. Engine erkennt: Spalte X erfüllt alle Schwellwerte
2. Index-Build wird in die Single-Writer Queue eingereiht
3. Sequentieller Scan über das .col File, B-Tree aufbauen
4. B-Tree wird als .btree File geschrieben
5. Pointer-Swap: Query Engine nutzt ab sofort den B-Tree
6. Eintrag in `_system.index_metrics`: Index erstellt, Grund, Zeitpunkt

### Performance
- Build ist ein sequentieller Scan über ein einziges .col File (Column-per-File Vorteil)
- 2 Mio Rows × 9 Bytes (z.B. int) = 18MB → Millisekunden
- Blockiert die Write-Queue kurz, aber nicht dramatisch

---

## Index-Erstellung (Manuell)

### Syntax
```
create index users.email
```

### Entfernen
```
purge index users.email
```

> Manuell erstellte Indizes werden vom Auto-System nicht angefasst.
> Kein automatisches Entfernen, kein Tracking der Nutzung für Auto-Entscheidungen.

---

## Index-Update bei Writes

- Bei jedem Insert/Update/Delete in der Single-Writer Queue:
  - Existiert ein .btree für betroffene Spalten? → B-Tree Update
  - B-Tree Insert/Update ist O(log n) → bei 2 Mio Rows ca. 21 Vergleiche
  - Nanosekunden-Bereich, WAL fsync dominiert die Write-Latenz bei weitem

---

## Index-Nutzung durch Query Engine

```
get users where email = 'john@test.com'

→ email hat .btree? → B-Tree Lookup → Place direkt
→ email hat kein .btree? → email.col Full Scan
```

> Column-per-File Vorteil: Index-Build liest nur ein einzelnes .col File, nicht die ganze Table.

---

## File-Struktur

```
users/
  _index
  _schema.json
  name.col
  email.col
  age.col
  email.btree        ← Auto- oder manueller Index
  age.btree           ← Auto-Index
```

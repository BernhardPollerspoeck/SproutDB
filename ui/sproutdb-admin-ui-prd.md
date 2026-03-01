# SproutDB Admin UI – Product Requirements Document

**Version:** 1.0  
**Datum:** 2026-03-01  
**Autor:** Bernhard Pollerspöck / QSP  
**Status:** Draft

---

## 1. Übersicht

SproutDB Admin ist eine eingebaute Web-Oberfläche für SproutDB v2. Sie wird als Razor Class Library im Core-Package mitgeliefert und mit einem Einzeiler aktiviert:

```csharp
app.MapSproutDBAdmin();
```

Kein separates Tool, kein Docker-Container, keine extra Installation. Selbe Auth wie die SproutDB HTTP API. Dark Mode als Default, Light Mode als Toggle.

**Technologie:** Blazor Server (läuft im selben Prozess, direkter Zugriff auf die SproutDB-Engine, SignalR ist bereits vorhanden).

**Zielgruppe:** Entwickler, Ops, Reporting-Nutzer – jeder der mit einer SproutDB-Instanz arbeitet.

---

## 2. Navigation & Layout

### 2.1 Topbar

| Element | Beschreibung |
|---|---|
| Logo | 🌱 SproutDB – klickbar, führt zurück zu Query |
| Nav-Buttons | Query, Schema, Monitoring, Admin |
| Status-Indikator | Grüner Dot = Verbindung aktiv, rot = getrennt |
| Server-Info | Version, Anzahl Databases, Anzahl Tables |
| Theme-Toggle | Dark/Light Mode. Preference wird im LocalStorage gespeichert |

### 2.2 Sidebar (links, 240px)

| Element | Beschreibung |
|---|---|
| Header | "Databases" Label + "+" Button für `create database` |
| Suchfeld | Filtert Tabellen live über alle Databases. Bei aktivem Filter klappen alle DBs auf |
| Database-Tree | Aufklappbare Database-Nodes mit Chevron-Icon. Zeigt Anzahl Tables als Badge |
| Table-Items | Klick auf Tabelle → Query-Editor wird mit `get tablename` vorbefüllt, DB-Selector passt sich an |

**Kein Row Count** im Sidebar-Tree. Row Count erscheint erst nach einer Query im Paging-Info.

### 2.3 Right Panel (rechts, 260px)

Immer sichtbar, unabhängig vom aktiven View. Drei Sektionen:

- **Live Metrics** – Writes/s, Reads/s, GB Storage, WAL MB
- **Auto-Index** – Aktive Indizes mit auto/manual Badge, Suggestions mit Progress-Bar
- **Activity Feed** – Letzte Operationen mit farbcodierten Dots (Insert=grün, Update=orange, Delete=rot, Get=blau)

---

## 3. Query View

Hauptview. Aufgeteilt in Query-Editor (oben) und Results-Bereich (unten).

### 3.1 Query Editor

| Element | Beschreibung |
|---|---|
| DB-Selector | Dropdown mit allen Databases. Bestimmt den Kontext für die Query |
| History-Button | Öffnet Session-basierte Query-History (nicht persistiert) |
| Auto-Paged Badge | `⚡ auto-paged · 100 rows` – zeigt dass Paging automatisch aktiv ist |
| Textarea | Monospace (JetBrains Mono), Syntax Highlighting für SproutDB Query Language |
| Keyboard Shortcut | `Ctrl+Enter` führt Query aus |
| Run-Button | Primärer grüner Button, führt Query aus |
| Format-Button | Formatiert die Query (nice-to-have, spätere Phase) |

### 3.2 Query Stats

Erscheinen nach erfolgreicher Query rechts neben den Buttons:

| Stat | Beschreibung | Beispiel |
|---|---|---|
| Time | Ausführungszeit der Query | `2.4ms` |
| Rows | Angezeigte Rows / Total Rows | `100 of 12,847` |
| Payload | Größe der Response | `51 KB` |

Bei Error: `ERR` in rot + Error-Code statt der normalen Stats.

### 3.3 Result-Tabs

Drei Tabs unterhalb des Editors:

#### 3.3.1 Results (Tabelle)

- **Spaltenheader** mit Typ-Badges (`ulong`, `string`, `ubyte`, `uint?` etc.)
- **Sort-Indikator** (▲/▼) bei sortierter Spalte
- **Farbcodierte Zellen:**
  - ID: grau (nicht editierbar)
  - String: grün
  - Number: orange
  - null: grau kursiv
  - Bool true: grün
  - Bool false: rot
  - DateTime: lila
- **Inline-Editing:** Hover über editierbare Zelle zeigt ✎-Icon. Klick öffnet Edit-Mode. Enter = Upsert absenden. Escape = abbrechen.
- **Zeilen-Highlight:** Frisch eingefügte/geänderte Rows flashen kurz grün (via SignalR Live-Update).

#### 3.3.2 JSON

Vollständiges SproutDB Response-Objekt mit Syntax Highlighting:

```
{
  "operation": 1,
  "data": [...],
  "affected": 100,
  "schema": null,
  "paging": { "page": 1, "pageSize": 100, "totalRows": 12847, "totalPages": 129 },
  "errors": null,
  "annotated_query": null
}
```

Kopierfähig. Zeigt das exakte JSON das die API zurückgibt.

#### 3.3.3 Schema

Zeigt das Schema der in der Query verwendeten Tabelle (entspricht `describe tablename`):

| Spalte | Beschreibung |
|---|---|
| Column | Spaltenname |
| Type | Datentyp mit Badge-Styling |
| Nullable | `not null` oder `nullable` (grün hervorgehoben) |
| Flags | PK, auto-increment, indexed, default-Werte |

### 3.4 Paging-Bar

| Element | Beschreibung |
|---|---|
| Page-Info | `Showing 1–100 of 12,847 rows · Page 1 of 129` |
| Page-Buttons | Prev, 1, 2, 3, …, 129, Next. Aktive Seite hervorgehoben |
| Next-Query | Vollständige Query für die nächste Seite, angezeigt in einem Read-Only-Feld mit Copy-Button |

**Next-Query Beispiel:**
```
get users where age > 25 order name select id, name, email, age, score page 2
```

Der User kann die Query kopieren und direkt in den Editor einfügen oder programmatisch verwenden.

### 3.5 Error-Darstellung

Bei Query-Fehlern wird statt der Ergebnis-Tabelle ein Error-Panel angezeigt:

| Element | Beschreibung |
|---|---|
| Error-Code | Rot, uppercase, z.B. `UNKNOWN_COLUMN` |
| Query mit Markierung | Die fehlerhafte Stelle wird rot mit wavy underline markiert |
| Annotated Query | Vollständige annotated_query aus dem Response mit `##...##` Markierungen |
| Available Columns | Liste aller verfügbaren Spalten als Badges (Hilfestellung für den User) |

---

## 4. Schema View

Übersichtsseite für alle Tabellen einer Database.

### 4.1 Layout

- **Header:** "Schema Browser" + DB-Badge (zeigt aktuell ausgewählte DB aus Sidebar)
- **Subtitle:** Anzahl Tables
- **Card-Grid:** Responsive Grid mit einer Card pro Tabelle

### 4.2 Schema-Card

Jede Card zeigt:

| Element | Beschreibung |
|---|---|
| Header | Table-Name mit blauem Icon |
| Spalten-Liste | Name + Typ-Badge pro Spalte |
| PK-Markierung | Gelbes "PK" Badge bei der ID-Spalte |
| Index-Markierung | ⚡-Icon bei indexierten Spalten, mit `manual` Label bei manuellen Indizes |

Klick auf eine Card → wechselt zum Query-View mit `describe tablename`.

---

## 5. Monitoring View

Dashboard mit Live-Metriken und Index-Status.

### 5.1 Metriken (oben, 4-spaltig)

| Metrik | Datenquelle | Update-Intervall |
|---|---|---|
| Writes/s | Engine-Counter (AtomicLong, Delta pro Sekunde) | 1s |
| Reads/s | Engine-Counter (AtomicLong, Delta pro Sekunde) | 1s |
| Total Storage | Summe aller .col Dateien + WAL. Gecacht | 10s |
| WAL Size | WAL-Dateigröße | 5s |

**Hinweis:** Writes/s und Reads/s erfordern einen neuen Counter im Engine-Code. Dieser muss als Feature geplant werden.

### 5.2 Storage per Database

Balkendiagramm pro Database:

| Element | Beschreibung |
|---|---|
| DB-Name | Links |
| Größe | Rechts (z.B. `1.8 GB`) |
| Bar | Proportional zur größten DB |

Datenquelle: Directory-Scan, gecacht, periodisch aktualisiert (nicht bei jedem UI-Refresh).

### 5.3 Auto-Index Status

Liste aller aktiven Indizes mit:

| Element | Beschreibung |
|---|---|
| Index-Name | z.B. `users.email` |
| Badge | `auto` (grün) oder `manual` (blau) |
| Selectivity | Prozentwert aus `_system.index_metrics` |
| Usage | Prozentwert aus `_system.index_metrics` |

**Suggestions:** Spalten die die Schwellenwerte fast erreichen (Usage >30%, Selectivity >95%) werden mit Progress-Bar und "create index?" Hinweis angezeigt.

---

## 6. Admin View

Aktions-orientierte Seite mit Card-Grid.

### 6.1 Actions

| Action | Beschreibung | Details |
|---|---|---|
| Export Database | ZIP-Download | Enthält alle .col Files, WAL, _schema.json, _migrations. Über `GET /sproutdb/admin/export/{db}` |
| Import Database | ZIP-Upload | Unzip in DB-Verzeichnis, Validierung der Schema-Dateien, Restart der betroffenen DB-Engine |
| Permissions | Verwaltung | Read/Write Permissions pro Database und Table. Abhängig vom Permission-System (spätere Phase) |
| Purge Database | Löschen | Confirmation-Dialog mit Eingabe des DB-Namens. Irreversibel |
| Settings | Konfiguration | WAL Sync Interval, Flush-Zyklus Intervall, Auto-Paging Default (pageSize), Auto-Index Schwellenwerte |
| Benchmark | Performance-Test | Insert/Get Benchmark gegen beliebige Tabelle. Zeigt ops/s und Latenz |

---

## 7. Realtime-Features

Basiert auf dem bereits vorhandenen SignalR Hub (`/sproutdb/changes`).

### 7.1 Live-Updates in Ergebnis-Tabelle

- UI subscribed auf `{database}.{table}` wenn Results angezeigt werden
- Neue/geänderte Rows flashen grün in der Tabelle
- Gelöschte Rows flashen rot und verschwinden (oder werden ausgegraut)

### 7.2 Activity Feed

- Alle Operationen über SignalR empfangen
- Farbcodierte Dots: Insert=grün, Update=orange, Delete=rot, Get=blau
- Format: `{OPERATION} → {db}.{table}` + relative Zeitangabe
- Max 50 Einträge, älteste fallen raus (Ring-Buffer)

### 7.3 Metriken-Updates

- Writes/s und Reads/s via SignalR oder Polling (1s Intervall)
- Storage und WAL-Größe via Polling (10s Intervall)

---

## 8. Keyboard Shortcuts

| Shortcut | Aktion |
|---|---|
| `Ctrl+Enter` | Query ausführen |
| `Ctrl+L` | Query-Editor fokussieren |
| `Escape` | Inline-Edit abbrechen |

---

## 9. Theme

### 9.1 Dark Mode (Default)

- Hintergrund: `#0a0e14` bis `#1f2733` (4 Abstufungen)
- Text: `#e6edf3` (primär) bis `#525c6a` (tertiär)
- Akzent: `#4ade80` (Sprout Green)
- Borders: `#252d3a`

### 9.2 Light Mode

- Invertierte Palette, Sprout Green bleibt als Akzent
- Preference im LocalStorage persistiert

### 9.3 Typografie

| Verwendung | Font |
|---|---|
| UI-Elemente | DM Sans |
| Code, Queries, Daten | JetBrains Mono |

---

## 10. Farbcodierung

Konsistente Farben in der gesamten UI:

| Farbe | Hex | Verwendung |
|---|---|---|
| Sprout Green | `#4ade80` | Primärakzent, Strings, Insert-Dots, Success |
| Blue | `#60a5fa` | Table-Icons, Read-Dots, manual Index |
| Orange | `#fb923c` | Numbers, Update-Dots, WAL |
| Red | `#f87171` | Errors, Delete-Dots, Bool false |
| Purple | `#c084fc` | DateTime-Werte |
| Yellow | `#fbbf24` | PK-Badge, Index-Icons, Suggestions, Pending |

---

## 11. Abhängigkeiten

Features die noch nicht in der SproutDB Engine existieren und für die Admin UI benötigt werden:

| Feature | Benötigt für | Aufwand |
|---|---|---|
| Read/Write Counter (AtomicLong) | Monitoring: Writes/s, Reads/s | Gering – zwei Counter im Engine, Delta pro Sekunde |
| `describe` Command | Schema-Tab, Schema-View | Query-Parser + Schema-Lookup. Gibt `_schema.json` Inhalt zurück |
| Admin-Endpoints | Export/Import/Purge/Settings | HTTP-Endpoints hinter Auth |
| Benchmark-Endpoint | Admin: Benchmark-Action | Insert/Get Loop mit Timing |
| Permission-System | Admin: Permissions | Eigenes Feature, unabhängig von UI |

---

## 12. Scope & Phasen

### Phase 1: Core UI

- Navigation (Query, Schema, Monitoring, Admin)
- Sidebar mit DB-Tree und Filter
- Query Editor mit Run, Stats, Auto-Paged Badge
- Results-Tabelle mit Typ-Badges, Farbcodierung, Paging, Next-Query
- JSON-Tab
- Error-Darstellung mit Annotated Query
- Dark/Light Mode

### Phase 2: Schema & Monitoring

- Schema-View mit Card-Grid
- Schema-Tab (per-table via `describe`)
- Monitoring mit Storage-Bars
- Monitoring mit Auto-Index Status und Suggestions

### Phase 3: Realtime & Editing

- Live-Updates in Ergebnis-Tabelle (SignalR)
- Activity Feed (SignalR)
- Live Metrics (Writes/s, Reads/s)
- Inline-Editing in der Ergebnis-Tabelle

### Phase 4: Admin

- Export/Import Database
- Purge Database mit Confirmation
- Settings-Verwaltung
- Benchmark-Tool

### Phase 5: DX Polish

- Syntax Highlighting im Query Editor
- Query History (Session-basiert)
- Format-Button
- Keyboard Shortcuts
- Available Columns bei Errors

---

## 13. Registrierung

```csharp
// ASP.NET
builder.Services.AddSproutDB(options => { ... });
app.MapSproutDB();           // HTTP + SignalR API
app.MapSproutDBAdmin();      // Admin UI (optional)

// Deaktiviert: einfach MapSproutDBAdmin() weglassen
```

Die Admin UI ist ein opt-in Feature. Keine zusätzlichen NuGet-Packages nötig. Alles im Core-Package enthalten.

---

## 14. Nicht im Scope

- Mobile-optimiertes Layout (Tablet reicht)
- Multi-User Editing / Collaboration
- Query-Ergebnisse als CSV/Excel exportieren (später)
- Custom Dashboard / gespeicherte Queries (später)
- Lokalisierung (Englisch only)

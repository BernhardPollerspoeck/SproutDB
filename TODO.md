# SproutDB Admin UI — Offene Punkte

Aufwand: `░` = 1 Punkt, 10 = max

## Query Page

### Result Tabs
- [ ] #T01 `███░░░░░░░` 3 **JSON-Tab** — Raw JSON Response anzeigen (kopierfähig, Syntax Highlighted). PRD 3.3.2
- [x] #T02 ~~**Schema-Tab**~~ — Nicht nötig, Schema Page existiert bereits.

### Paging
- [ ] #T03 `█████░░░░░` 5 **Paging-Bar** — "Showing 1–100 of 12,847 rows · Page 1 of 129", Prev/Next Buttons. PRD 3.4
- [ ] #T04 `██░░░░░░░░` 2 **Next-Query** — Vollständige Query für nächste Seite in Read-Only-Feld mit Copy-Button. PRD 3.4
- [ ] #T05 `██░░░░░░░░` 2 **Auto-Paging Info** — Badge zeigt aktuelles Paging (`100 rows`), reagiert auf tatsächliche Response-Paging-Daten

### Error UX
- [ ] #T06 `███░░░░░░░` 3 **Available Columns bei Errors** — Bei UNKNOWN_COLUMN die verfügbaren Spalten als Badges anzeigen. PRD 3.5

### Editor
- [ ] #T07 `██████░░░░` 6 **Format-Button** — Query formatieren (aktuell Placeholder, tut nichts). PRD 3.1
- [x] #T08 `█░░░░░░░░░` 1 **Alt+L** — Editor fokussieren. PRD 8

### Tabellen-UX
- [ ] #T09 `██░░░░░░░░` 2 **Sort-Indikator** — Dreiecke in Spaltenheadern bei sortierter Query (order by). PRD 3.3.1

## Monitoring Page

- [ ] #T10 `████░░░░░░` 4 **Auto-Index Status** — Aktive Indizes mit auto/manual Badge, Selectivity/Usage aus index_metrics. PRD 5.3
- [ ] #T11 `█████░░░░░` 5 **Auto-Index Suggestions** — Spalten die Schwellenwerte fast erreichen, mit Progress-Bar. PRD 5.3

## Nice-to-have (spätere Phase)

- [ ] #T12 `████░░░░░░` 4 CSV/Excel Export der Query-Ergebnisse. PRD 14
- [ ] #T13 `███████░░░` 7 Gespeicherte Queries / Custom Dashboard. PRD 14

---

## Autocomplete — Test-Matrix

Legende: ✅ = Test + Logik vorhanden, ❌ = fehlt komplett, ⚠️ = Logik fehlt (Test wäre rot)

### GET

| # | Input (⎸ = Cursor) | Erwarteter Typ | Erwartete Items | Status |
|---|---|---|---|---|
| A01 | `⎸` | command | get, upsert, delete, ... | ✅ |
| A02 | `ge⎸` | command | get (prefix-filter) | ✅ |
| A03 | `get ⎸` | table | (aus Schema) | ✅ |
| A04 | `get us⎸` | table | users (prefix-filter) | ✅ |
| A05 | `get users ⎸` | clause | where, select, order, ... | ✅ |
| A06 | `get users wh⎸` | clause | where (prefix-filter) | ✅ |
| A07 | `get users where ⎸` | column | (aus Schema) | ✅ |
| A08 | `get users where name ⎸` | operator | =, !=, >, contains, ... | ✅ |
| A09 | `get users where active = ⎸` | boolean | true, false, null | ✅ |
| A10 | `get users where name = 'x' and ⎸` | column | (aus Schema) | ❌ |
| A11 | `get users where name = 'x' or ⎸` | column | (aus Schema) | ❌ |
| A12 | `get users where name = 'x' and age ⎸` | operator | =, !=, ... | ✅ |
| A13 | `get users select ⎸` | column | (aus Schema) | ✅ |
| A14 | `get users select name, ⎸` | column | (aus Schema) | ⚠️ |
| A15 | `get users order by ⎸` | column | (aus Schema) | ✅ |
| A16 | `get users order by name ⎸` | direction | asc, desc | ✅ |
| A17 | `get users order ⎸` | column/by? | by oder column | ⚠️ |
| A18 | `get users group ⎸` | column | (aus Schema) — by optional | ⚠️ |
| A19 | `get users group by ⎸` | column | (aus Schema) | ❌ |
| A20 | `get users limit ⎸` | none | (Zahl erwartet, keine Suggestion) | ❌ |
| A21 | `get users page ⎸` | none | (Zahl erwartet, keine Suggestion) | ❌ |
| A22 | `get users page 1 ⎸` | clause | size (+ andere) | ⚠️ |
| A23 | `get users follow ⎸` | none/table.col | (follow ist komplex) | ⚠️ |
| A24 | `get users sum ⎸` | column | (aus Schema) — Aggregate | ⚠️ |
| A25 | `get users -select ⎸` | column | (aus Schema) — Exclude | ⚠️ |
| A26 | `get users where age between ⎸` | none | (Wert erwartet) | ❌ |
| A27 | `get users where status in ⎸` | none | (Liste erwartet) | ❌ |
| A28 | `get users where name is ⎸` | none/kw | null, not null | ⚠️ |
| A29 | `get users select name from ⎸` | table | (aus Schema) | ✅ |
| A30 | `get users distinct ⎸` | clause | (nächste Clause) | ❌ |
| A31 | `get users count ⎸` | clause | (nächste Clause) | ❌ |

### UPSERT

| # | Input | Erwarteter Typ | Erwartete Items | Status |
|---|---|---|---|---|
| A32 | `upsert ⎸` | table | (aus Schema) | ✅ |
| A33 | `upsert users ⎸` | upsert-body | { }, [ ] | ✅ |
| A34 | `upsert users { ⎸` | column | (aus Schema) | ✅ |
| A35 | `upsert users { name: 'x', ⎸` | column | (aus Schema) | ✅ |
| A36 | `upsert users { name: ⎸` | none | (Wert erwartet) | ✅ |
| A37 | `upsert users [{ ⎸` | column | (aus Schema) | ✅ |
| A38 | `upsert users [{ name: 'x', ⎸` | column | (aus Schema) | ✅ |
| A39 | `upsert users [ ⎸` | none | (innerhalb Bracket, kein Brace) | ✅ |
| A40 | `upsert users { name: 'x' } ⎸` | upsert-after | on | ✅ |
| A41 | `upsert users [{ name: 'x' }] ⎸` | upsert-after | on | ✅ |
| A42 | `upsert users { name: 'x' } on ⎸` | column | (aus Schema) | ✅ |

### DELETE

| # | Input | Erwarteter Typ | Erwartete Items | Status |
|---|---|---|---|---|
| A43 | `delete ⎸` | table | (aus Schema) | ✅ |
| A44 | `delete users ⎸` | clause | where (einzige sinnvolle) | ❌ |
| A45 | `delete users where ⎸` | column | (aus Schema) | ❌ |
| A46 | `delete users where name = 'x' and ⎸` | column | (aus Schema) | ❌ |

### DESCRIBE

| # | Input | Erwarteter Typ | Erwartete Items | Status |
|---|---|---|---|---|
| A47 | `describe ⎸` | table | (aus Schema) | ✅ |
| A48 | `describe us⎸` | table | users (prefix-filter) | ❌ |

### CREATE

| # | Input | Erwarteter Typ | Erwartete Items | Status |
|---|---|---|---|---|
| A49 | `create ⎸` | create-sub | database, table, index, apikey | ✅ |
| A50 | `create ta⎸` | create-sub | table (prefix-filter) | ❌ |
| A51 | `create table ⎸` | none | (freier Name) | ❌ |
| A52 | `create index ⎸` | none/table.col | (table.col erwartet) | ❌ |

### PURGE

| # | Input | Erwarteter Typ | Erwartete Items | Status |
|---|---|---|---|---|
| A53 | `purge ⎸` | purge-sub | database, table, column, index, apikey | ✅ |
| A54 | `purge table ⎸` | table | (aus Schema) | ✅ |
| A55 | `purge column ⎸` | none/table.col | (table.col erwartet) | ❌ |
| A56 | `purge index ⎸` | none/table.col | (table.col erwartet) | ❌ |
| A57 | `purge da⎸` | purge-sub | database (prefix-filter) | ❌ |

### ADD / ALTER / RENAME COLUMN

| # | Input | Erwarteter Typ | Erwartete Items | Status |
|---|---|---|---|---|
| A58 | `add column ⎸` | none/table.col | (table.col erwartet) | ❌ |
| A59 | `add column users.email ⎸` | type | string, bool, sint, ... | ✅ |
| A60 | `alter column ⎸` | none/table.col | (table.col erwartet) | ❌ |
| A61 | `rename column ⎸` | none/table.col | (table.col erwartet) | ❌ |
| A62 | `rename column users.old to ⎸` | none | (freier Name) | ❌ |

### AUTH (grant / revoke / restrict / unrestrict / rotate)

| # | Input | Erwarteter Typ | Erwartete Items | Status |
|---|---|---|---|---|
| A63 | `grant ⎸` | none/role | admin, writer, reader | ⚠️ |
| A64 | `grant writer on ⎸` | none/db | (DB-Name erwartet) | ⚠️ |
| A65 | `revoke ⎸` | none/db | (DB-Name erwartet) | ⚠️ |
| A66 | `restrict ⎸` | none/table | (Table oder * erwartet) | ⚠️ |
| A67 | `unrestrict ⎸` | none/table | (Table erwartet) | ⚠️ |
| A68 | `rotate apikey ⎸` | none | ('<name>' erwartet) | ⚠️ |

### BACKUP / RESTORE

| # | Input | Erwarteter Typ | Erwartete Items | Status |
|---|---|---|---|---|
| A69 | `backup⎸` | command | backup (prefix-filter) | ❌ |
| A70 | `restore ⎸` | none | ('<path>' erwartet) | ❌ |

---

## Offene Design-Fragen (Autocomplete)

### F1: `and`/`or` nach WHERE-Bedingung

**Aktuell:** Nach `get users where name = 'bob' ⎸` kommt `clause` (where, select, order, ...).
**Problem:** `and` und `or` stehen nicht in der CLAUSES-Liste. Der User bekommt keine Hilfe um die Bedingung zu erweitern.
**Option A:** `and`/`or` in CLAUSES aufnehmen — dann erscheinen sie aber auch an Stellen wo sie keinen Sinn machen (z.B. `get users ⎸`).
**Option B:** Eigener Context-Typ `where-continuation` der nur nach einer vollständigen WHERE-Bedingung greift und `and`, `or` + die regulären Clauses (select, order, ...) anbietet.
**Option C:** Einfach nicht — `and`/`or` sind kurz genug zum Tippen.

### F2: Komma-getrennte Spalten (`select name, ⎸`)

**Aktuell:** Nach `select name, ⎸` erkennt der Autocomplete das Komma nicht und liefert `clause` statt `column`.
**Problem:** Kommas kommen in SproutDB-Syntax an mehreren Stellen vor: `select col1, col2`, `group by col1, col2`, `order by col1 asc, col2 desc`.
**Lösung:** Nach einem Komma prüfen ob der letzte Clause-Kontext `select`, `group` oder `order` war und dann erneut `column` vorschlagen. Das erfordert Backtracking im Token-Stream um den aktiven Clause zu finden.

### F3: Auth-Befehle (grant, revoke, restrict, unrestrict, rotate)

**Aktuell:** Diese Befehle werden als `command` vorgeschlagen, aber danach gibt es keine weitere Hilfe.
**Kontext:** Auth-Befehle werden selten getippt — meistens über die Admin-UI-Buttons. Im Query-Editor sind sie eher für Power-User.
**Option A:** Vollständiges Autocomplete für Auth (Rollen, DB-Namen, Key-Namen). Aufwand ~6 weil die Syntax verschachtelt ist (`grant <role> on <db> to '<key>'`).
**Option B:** Nur die Commands vorschlagen, keine Argumente. Power-User kennen die Syntax.
**Option C:** Auth-Commands ganz aus dem Autocomplete entfernen (sie machen im normalen Query-Editor wenig Sinn).

### F4: `follow` Clause

**Aktuell:** `follow` steht in CLAUSES und wird vorgeschlagen, aber danach kommt keine Hilfe.
**Syntax:** `follow users.id -> orders.user_id as orders [where ...]`
**Problem:** Follow erwartet `table.column -> table.column as alias` — das ist eine komplexe Sequenz. Der Autocomplete müsste:
1. Nach `follow ⎸` → `table.column` vorschlagen (alle Tabellen + deren Spalten)
2. Nach `follow users.id ⎸` → `->` vorschlagen
3. Nach `-> ⎸` → `table.column` vorschlagen
4. Nach `-> orders.user_id ⎸` → `as` vorschlagen
5. Nach `as ⎸` → freier Alias
6. Danach optional `where` mit Sub-Bedingungen
**Option A:** Vollständig implementieren (Aufwand ~5-6).
**Option B:** Nur `follow` als Keyword vorschlagen, Rest tippt der User. Follow ist ein Advanced Feature.

### F5: Aggregat-Funktionen (`sum`, `avg`, `min`, `max`)

**Aktuell:** Aggregate stehen nicht in CLAUSES. Nach `get users ⎸` werden sie nicht vorgeschlagen.
**Syntax:** `get users sum amount` / `get users avg score as avg_score`
**Problem:** Aggregate stehen syntaktisch an derselben Position wie Clauses (nach `get <table>`), sind aber semantisch anders — sie erwarten direkt eine Spalte danach.
**Option A:** Aggregate in CLAUSES aufnehmen. Danach `column`-Suggestions. Einfachste Lösung.
**Option B:** Eigener Context `aggregate` nach `get <table>`, der sowohl Aggregates als auch Clauses anbietet. Mehr Arbeit, sauberer.
**Option C:** Nicht implementieren — Aggregates sind selten genug.

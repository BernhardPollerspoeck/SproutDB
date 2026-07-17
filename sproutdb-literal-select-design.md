# Design: Literal-Spalten in der Select-Liste

Status: **Implementiert.** 1470 Tests grün, 100% Line- und Branch-Coverage der geänderten Zeilen.

## Abweichungen vom Entwurf (beim Bauen entdeckt)

Drei Dinge kamen erst bei der Umsetzung raus. Alle drei sind abgestimmt:

1. **Follow-Target-Select wirft keinen Fehler mehr, sondern wird zum Post-Follow-Select.**
   `IsPostFollowSelect` unterschied Follow-Level von Post-Follow allein an einem Dot-Token —
   `follow ... as b select host, true as ha` (kein Dot) landete deshalb beim Follow-Target-Parser.
   Damit war die Scope-Entscheidung "Basis + Post-Follow" gar nicht erfüllt: Post-Follow-Literale
   gingen nur, wenn zufällig eine `alias.col`-Spalte in der Liste stand. Jetzt gelten **Literal,
   Arithmetik und Dot** gleichermaßen als Post-Follow-Signal — ein Follow-Level-Select kann keins
   davon ausdrücken. Der geplante Fehler `literals are not allowed in a follow select` entfällt
   ersatzlos, weil kein Literal den Follow-Level-Parser mehr erreicht.

2. **Mitgefixt: `ParseFollowSelectList` kannte nur zwei der neun Endewörter.** Es stoppte bei
   `where`/`follow`, während alle anderen Select-Parser `SelectStopKeywords` (9 Wörter) nutzen.
   Folge waren bestehende Bugs: `follow ... select name limit 5` las `limit` als Spaltennamen und
   starb dann an der `5`; `follow ... select name count` lieferte stillschweigend eine Spalte
   namens `count`. Beide benutzen jetzt dieselbe Liste.

3. **Aggregat + Literal ist grammatikalisch unmöglich** — bestätigt, nicht nur vermutet. Das
   Aggregat steht vor dem Select (`get t sum port as total`), und der Parser überspringt den
   Select-Block, sobald er eins sieht (`GetParser.cs:94`). Der Aggregat-Zweig von `ExecuteGrouped`
   kann ein Literal also nie sehen; die Injektion dort bedient nur den Count- und den
   Ohne-Aggregat-Zweig.

## Folge-Fix: Order-By auf nicht projizierter Spalte (nachträglich, separat entschieden)

Beim Bauen gefunden, danach als eigener Fix umgesetzt ("Weg 2" — ablehnen statt still ignorieren):

- **Vorher:** `get routes select host order by port desc` lief fehlerfrei durch und sortierte still
  gar nicht — der Sort läuft auf den projizierten Zeilen, `TryGetValue("port")` fand nichts, alle
  Vergleiche lieferten 0.
- **Jetzt:** `UNKNOWN_COLUMN: 'order by port' requires 'port' in the select list`. Gilt auch für
  `-select port order by port`, distinct und `order by _id` ohne Limit. **Breaking** für Queries,
  die vorher (unsortiert) durchliefen.
- **Ausnahmen**, wo es ohne die Spalte korrekt funktioniert: `order by _id [desc] limit N`
  (Top-N-Fast-Path), `order by _id` mit `after` (Cursor sortiert konstruktionsbedingt), `count`
  (keine Zeilen, Sortierung gegenstandslos), Follow-`alias.col` (eigene Keys).
- **Mitgefixt:** `select port as p order by p` wurde von der alten Validierung fälschlich als
  `UNKNOWN_COLUMN` abgelehnt, obwohl der Key `p` in der Row existiert — Select-Aliase stehen jetzt
  in der Order-By-Whitelist. Umgekehrt ist `select port as p order by port` jetzt ein Fehler (der
  Key heißt `p`, nicht mehr `port`).
- Bewusst **nicht** gewählt: "Weg 1" (fehlende Order-By-Spalten temporär mitlesen wie bei Computed
  Columns) — hätte Exclude- und Distinct-Fälle still kaputt gelassen und mehr Code gekostet.

## Problem

Die Select-Liste akzeptiert heute ausschließlich Spaltennamen:

```
get routes select host, true as preserve_host      →  UNKNOWN_COLUMN: column 'true' does not exist
get routes select host, 'auto' as backend_protocol →  SYNTAX_ERROR: expected column name
get routes select host, 1 as eins                  →  SYNTAX_ERROR: expected column name
```

Ursache: `ParseSelectList` (`GetParser.cs:663-668`) verlangt für jedes Element ein `Identifier`-Token.
`'auto'` und `1` scheitern dort. `true` ist bei uns ein Identifier-Token (`GetParser.cs:640`), kommt
also durch den Parser und fliegt erst im Executor als `UNKNOWN_COLUMN` raus (`GetExecutor.cs:18-22`) —
anderer Fehler, gleiche Ursache.

Ziel: konstante Werte projizieren können, um Result-Shapes zwischen Queries anzugleichen
(z.B. Discriminator-Felder, Defaults für Spalten die es in dieser Tabelle nicht gibt).

## Entscheidungen

| Frage | Entscheidung |
|-------|--------------|
| Alias | **Pflicht.** Ohne Alias gibt es keinen sinnvollen Output-Key. |
| `true`/`false`/`null` | **Nur mit `as` ein Literal.** `select true as x` → Literal; `select true` → Spaltenname (Verhalten bleibt wie heute). |
| Typen | String, Integer, Float, Bool, null — alle. |
| Scope | Basis-Select **und** Post-Follow-Select. Follow-Target-Select (`follow ... select`) **nicht** — Begründung unten. |
| Injektion | **Als `ProjectionEntry` an der Select-Position** ("Weg 1"), nicht als Nachlauf-Injektion. |
| Key-Reihenfolge | **Erhalten**, soweit der Pfad sie überhaupt garantiert (s. Einschränkung unten). |
| Right/Outer-Robustheit | `ExecuteFollow` bekommt die Alias→Wert-Map, Nulling schreibt den Wert statt `null`. |
| `group by` | Literal wird injiziert (konstant pro Gruppen-Row). |
| Post-Follow-Select | Basis-Literal überlebt einen expliziten Post-Follow-Select **nicht** — wie Basis-Computed heute. |
| Duplikat-Aliase | **Fehler, sobald ein `as` beteiligt ist.** `select host, host` bleibt erlaubt. |

Konsequenz aus der `true`/`false`/`null`-Regel: eine Spalte namens `true`/`false`/`null` lässt sich
nicht mehr aliasen (`select true as x` liefert dann das Literal, nicht die Spalte). Unaliast bleibt
sie erreichbar. Akzeptierter Trade-off — pathologischer Fall.

## Syntax

```
get routes select host, true as preserve_host, 'auto' as backend_protocol, 1 as version
  → [{ host: '...', preserve_host: true, backend_protocol: 'auto', version: 1 }, ...]

get routes select host, null as cert_path
  → [{ host: '...', cert_path: null }, ...]

get routes select -1 as offset
  → [{ offset: -1 }, ...]

get routes follow routes._id -> backends.route_id as b
  select host, b.name, true as ha
  → Post-Follow, funktioniert identisch
```

Fehlerfälle:

```
get routes select host, true                →  UNKNOWN_COLUMN: column 'true' does not exist   (unverändert)
get routes select host, 'auto'              →  SYNTAX_ERROR: literal in select requires an alias
get routes -select host, 1 as x             →  SYNTAX_ERROR: literals are not allowed in '-select'
get routes select host as x, 1 as x         →  SYNTAX_ERROR: duplicate output name 'x'
get routes follow ... as b select 1 as x    →  SYNTAX_ERROR: literals are not allowed in a follow
                                               select — use the select after the follow clause
```

### Abgrenzung zu Computed Columns

`-1 as offset` kollidiert nicht mit `-select`: das Minus in `-select` steht **vor** dem Keyword
und wird in `GetParser.cs:101-118` konsumiert, bevor `ParseSelectList` überhaupt läuft.

`select price -1 as x` (ohne Komma) bleibt eine Computed Column (`price - 1`) — das Komma
trennt Literal von Arithmetik. Bestehendes Verhalten, unverändert.

Literale sind **keine** Operanden: `select true * 2 as x` bleibt ein `UNKNOWN_COLUMN`-Fehler
auf `true`. Der bestehende `ComputedColumn.RightLiteral`-Pfad (`price * 1.2 as gross`) ist
davon unberührt. Literal-Aliase sind auch nicht als Follow-Source verwendbar
(`follow routes.v -> ...` mit Literal `v`) — dort greift weiter `UNKNOWN_COLUMN`, wie bei
Computed-Aliasen.

### Warum kein Follow-Target-Select

Drei Gründe, der erste ist der eigentliche:

1. **Es bringt keine Ausdruckskraft.** Follow ist ein flacher Join (`GetExecutor.cs:286`), Keys sind
   `alias.col`. Ein Literal ist konstant — es hängt per Definition nicht von der gejointen Row ab.
   `follow ... select 1 as x` und Post-Follow `select 1 as x` liefern dieselbe Konstante in jeder
   Row. Einziger Unterschied wäre der `b.`-Prefix am Key.
2. **Der Match-Indikator-Fall ist schon abgedeckt.** Das einzige, was ein Follow-Target-Literal
   könnte und Post-Follow nicht (`true as matched` → `null` wenn kein Match, via
   `BuildNullTargetRow`, `:935-939`), liefert `b._id` bereits: non-null bei Match, null ohne
   (`:929`). `follow ... as b select _id, name` und man hat es.
3. **Eigener, ärmerer Parser.** Der Follow-Target-Select geht durch `ParseFollowSelectList`
   (`:1240`), nicht `ParseSelectList`. Der kann nur `spalte [as alias]` — **keine Computed Columns**.
   Literale dort einzubauen hieße, der schwächeren Klausel etwas zu geben, das ihre mächtigere
   Schwester dort nicht hat.

## Datenmodell

Neu in `GetQuery.cs` — analog zu `ComputedColumn`, **nicht** als Flag in `SelectColumn`:

```csharp
internal sealed class LiteralColumn
{
    /// <summary>Konstanter Wert: string, long, double, bool oder null.</summary>
    public required object? Value { get; init; }

    /// <summary>Alias für den Output-Key. Pflicht.</summary>
    public required string Alias { get; init; }

    /// <summary>Position/Länge des Literal-Tokens im Query-String. Position trägt zusätzlich
    /// die Sortierung für die Key-Reihenfolge (Merge mit SelectColumn.Position).</summary>
    public int Position { get; init; }
    public int Length { get; init; }
}
```

Begründung gegen ein `IsLiteral`-Flag in `SelectColumn`: `SelectColumn` ist ein readonly struct,
der in Validierung, Projektion, Group-By und Order-By konsumiert wird. Ein Flag zwingt **jeden**
dieser Consumer zu einem zusätzlichen Check — und jeder vergessene Check wird zu einem
`UNKNOWN_COLUMN` auf einem Literal. Eine separate Liste folgt dem etablierten `ComputedSelect`-Muster
und ist für alle bestehenden Pfade unsichtbar.

Zwei neue Properties in `GetQuery`:

```csharp
public List<LiteralColumn>? LiteralSelect { get; init; }
public List<LiteralColumn>? PostFollowLiteralSelect { get; init; }
```

Typ-Mapping: `IntegerLiteral` → `long`, `FloatLiteral` → `double`, `StringLiteral` → `string`,
`true`/`false` → `bool`, `null` → `null`. Alle serialisieren ohne Sonderbehandlung in die
JSON-Array-Response.

## Parser-Änderungen (`GetParser.cs`)

### 1. Signatur

`ParseSelectList` gibt ein 3-Tupel zurück und kennt den Exclude-Modus:

```csharp
private static (List<SelectColumn> Columns, List<ComputedColumn>? Computed, List<LiteralColumn>? Literals)
    ParseSelectList(ParserContext ctx, bool isExclude)
```

Vier Call-Sites: `:98, :109, :175, :185` (Basis-Select, Basis `-select`, Post-Follow-Select,
Post-Follow `-select`). Bei `isExclude: true` → Literal-Erkennung greift, erzeugt aber sofort
`SYNTAX_ERROR: literals are not allowed in '-select'`. (Nicht einfach durchlaufen lassen — sonst
wird `-select 1 as x` zu `expected column name` und der Nutzer rät.)

### 2. Literal-Erkennung im Schleifenkopf

Vor dem `token.Type != TokenType.Identifier`-Check (`:663`):

```csharp
if (IsLiteralStart(ctx, token))
{
    var lit = ParseLiteralColumn(ctx, isExclude);
    if (ctx.HasErrors) return (columns, computed, literals);
    if (lit is not null)
    {
        literals ??= [];
        literals.Add(lit);
    }
    // danach: Komma-Handling wie gehabt
}
```

`IsLiteralStart`:

| Token | Literal? |
|-------|----------|
| `StringLiteral`, `IntegerLiteral`, `FloatLiteral` | ja |
| `Minus` + `IntegerLiteral`/`FloatLiteral` (Lookahead 1) | ja |
| `Identifier` `true`/`false`/`null` **und** `PeekAt(1)` ist Keyword `as` | ja |
| alles andere | nein → bestehender Pfad |

Der Zwei-Token-Lookahead beim dritten Fall ist die Umsetzung der `as`-Entscheidung:
`ctx.IsKeyword(token, "true") && ctx.IsKeyword(ctx.PeekAt(1), "as")`.

### 3. `ParseLiteralColumn`

Wert parsen + konsumieren, dann `as` **verpflichtend**:

```csharp
if (!ctx.MatchKeyword("as"))
    → SYNTAX_ERROR "literal in select requires an alias"   (Position = Literal-Token)
var aliasToken = ctx.Peek();
if (aliasToken.Type != TokenType.Identifier)
    → SYNTAX_ERROR "expected alias name after 'as'"        (bestehende Message)
```

Formulierung angelehnt an die bestehende Computed-Meldung `computed field requires 'as <alias>'`
(`:858`).

Die Wert-Parse-Logik ähnelt `:606-643`, gibt dort aber die **String-Repräsentation** für WHERE
zurück. Hier brauchen wir typisierte `object?`-Werte für die Row-Dictionaries — also eine eigene
kleine Funktion, keine Wiederverwendung.

### 4. Leere-Liste-Check

`:764` muss Literale mitzählen, sonst ist `get t select 1 as x` ein Fehler:

```csharp
if (columns.Count == 0 && computed is null && literals is null)
    ctx.AddError(..., ErrorMessages.EXPECTED_COLUMN_NAME);
```

### 5. Duplikat-Prüfung (neu, **potentiell breaking**)

Am Ende von `ParseSelectList`, nur wenn `!isExclude`. Über alle Output-Namen der drei Listen:

- `SelectColumn.OutputName` (Alias oder Spaltenname)
- `ComputedColumn.Alias`
- `LiteralColumn.Alias`

Regel: ein Name, der mehrfach vorkommt, ist ein Fehler **genau dann, wenn mindestens einer der
kollidierenden Einträge einen expliziten Alias trägt.**

```
select host, host                  → ok      (beide ohne Alias — harmlose Wiederholung)
select host, 1 as host             → FEHLER  (Literal aliased)
select host as x, price as x       → FEHLER
select price * 2 as x, qty * 3 as x → FEHLER  ← heute erlaubt, bricht
-select host, host                 → ok      (Exclude: keine Prüfung, Alias dort bedeutungslos)
```

Fehler: `SYNTAX_ERROR: duplicate output name '<name>'`, Position = zweites Vorkommen.

Post-Follow-Liste wird getrennt geprüft (eigener Namensraum).

**Breaking:** `select price * 2 as x, qty * 3 as x` ist heute erlaubt (stilles Last-Writer-Wins)
und wird zum Fehler. Bewusst so entschieden — gehört in die Release-Notes.

### 6. Follow-Target: expliziter Fehler statt stillem Break

`ParseFollowSelectList` (`:1240`) bricht bei `token.Type != Identifier` **kommentarlos** ab
(`:1246-1247`). Damit würde `follow ... as b select 1 as x` das Literal still verschlucken,
`1 as x` bliebe im Token-Stream liegen und knallte erst bei `ctx.ExpectEof()` als kryptischer
Fehler an der falschen Position.

Muss unabhängig von der Scope-Entscheidung repariert werden:

```csharp
if (IsLiteralStart(ctx, token))
{
    ctx.AddError(token, ErrorCodes.SYNTAX_ERROR,
        "literals are not allowed in a follow select — use the select after the follow clause");
    return columns;
}
```

Nachrangige Falle: `IsPostFollowSelect` (`:1216-1235`) unterscheidet Follow-Level von Post-Follow
per Dot-Suche. `follow ... select 1 as x` hat keinen Dot → gilt als Follow-Level → landet in
`ParseFollowSelectList` → obiger Fehler greift. Korrekt, aber der Grund gehört als Kommentar dran.

## Executor-Änderungen (`GetExecutor.cs`)

### Validierung

Für Literale **keine** — sie referenzieren kein Schema.

Eine Anpassung nötig: die Order-By-Whitelist (`:55-67`) sammelt Computed-Aliase, damit
`order by <alias>` nicht als `UNKNOWN_COLUMN` abgelehnt wird. Literal-Aliase (Basis + Post-Follow)
müssen in dieselbe Menge. Sortieren nach einer Konstante ist ein No-Op — aber `order by preserve_host`
mit "column does not exist" abzulehnen, obwohl die Spalte im Result steht, wäre verwirrend.

### Projektion: Literal als vierte `ProjectionEntry`-Sorte

Das ist der Kern. `ResolveProjection` (`:1188-1233`) baut laut Doc-Kommentar "an ordered projection
list that respects the SELECT column order" und nutzt bereits `col.OutputName`. Ein Literal ist
schlicht ein weiterer Entry-Typ:

```csharp
private enum VirtualColumn : byte { None, Id, ExpiresAt, Ttl, Literal }   // + Literal

private readonly record struct ProjectionEntry(
    string Name, ColumnHandle? Handle,
    VirtualColumn Virtual = VirtualColumn.None,
    object? LiteralValue = null);                                          // + LiteralValue
```

`ProjectRow` (`:1288-1294`) bekommt einen Zweig im bestehenden Switch — kein neuer Dispatch:

```csharp
record[entry.Name] = entry.Virtual switch
{
    VirtualColumn.Id => id,
    VirtualColumn.ExpiresAt => ttl?.ReadExpiresAt(place) ?? 0L,
    VirtualColumn.Ttl => ttl?.ReadRowTtlDuration(place) ?? 0L,
    VirtualColumn.Literal => entry.LiteralValue,        // neu
    _ => null,
};
```

`ResolveProjection` bekommt die Literal-Liste als Parameter und merged sie im Nicht-Exclude-Zweig
(`:1220-1232`) nach `Position` in die Select-Liste. Beide Listen sind bereits in Quelltext-Reihenfolge
→ Zwei-Zeiger-Merge, kein Sortieren.

Damit erben **alle** Row-Lese-Pfade die Literale automatisch, weil alle über `ResolveProjection`
gehen:

| Funktion | Call-Sites |
|----------|-----------|
| `ProjectPlaces` (`:633`) | `:228` (Top-N), `:514` (Cursor) |
| `ReadRows` (`:1142`) | `:235` |
| `ReadRowsByPlaces` (`:1527`) | `:234` |

Alle drei bekommen `List<LiteralColumn>?` durchgereicht. **Kein** separater
`ApplyLiteralColumns`-Aufruf im Haupt- oder Cursor-Pfad nötig.

### Right/Outer-Join: Literale nicht nullen

`ExecuteFollow` (`:866`) nullt im Right/Outer-Zweig alle Source-Keys für unmatched Target-Rows
(`:946-956`, `sourceColumns = data[0].Keys.ToList()` → `flat[col] = null`). Ein Basis-Literal ist
zu dem Zeitpunkt ein Source-Key und würde mitgenullt — das Literal wäre nicht mehr konstant:

```
get routes select 1 as v follow routes._id ?-> backends.route_id as b
  → [{ v: 1,    b.name: 'x' },   ← gematcht
     { v: null, b.name: 'y' }]   ← unmatched backend — falsch
```

Fix: `ExecuteFollow` bekommt `Dictionary<string, object?>? literalValues` (Alias→Wert, einmal aus
`q.LiteralSelect` gebaut, nur für Right/Outer relevant). Zeile 955-956 wird zu:

```csharp
foreach (var col in sourceColumns)
    flat[col] = literalValues is not null && literalValues.TryGetValue(col, out var lv) ? lv : null;
```

Call-Site: `:295`.

### Post-Follow-Literale

Die Post-Follow-Projektion (`:354-375`) baut `projected` aus `PostFollowSelect` neu auf und würde
Literale verwerfen. Sie muss stattdessen über eine nach `Position` gemergte Liste aus
`PostFollowSelect` + `PostFollowLiteralSelect` laufen — dieselbe Zwei-Zeiger-Logik wie in
`ResolveProjection`, damit auch hier die Reihenfolge stimmt:

```csharp
// je Eintrag der gemergten Liste:
//   SelectColumn  → if (row.TryGetValue(col.Name, out var val)) projected[col.OutputName] = val;
//   LiteralColumn → projected[lit.Alias] = lit.Value;
```

Der bestehende Block für Post-Follow-Computed-Aliase (`:363-372`, "Computed aliases travel with the
row") bleibt unangetastet.

**Basis-Literale überleben einen expliziten Post-Follow-Select nicht** — sie stehen nicht in
`PostFollowSelect`, also greift `:358-361` und der Key fällt raus. Exakt das heutige Verhalten von
Basis-Computed-Columns. Wer ihn behalten will, listet ihn auf:

```
get routes select 1 as v follow routes._id -> backends.route_id as b select host
  → [{ host: '...' }]              ← v weg
get routes select 1 as v follow routes._id -> backends.route_id as b select host, v
  → [{ host: '...', v: 1 }]        ← explizit gelistet
```

### `group by`

`ExecuteGrouped` (`:696-751`) baut Rows manuell aus Group-Key-Spalten + Aggregat und geht **nicht**
über `ResolveProjection` — erbt die Literale also nicht. Ein expliziter
`ApplyLiteralColumns(data, q.LiteralSelect)` vor dem Return, sonst verschwindet das Literal
kommentarlos:

```csharp
private static void ApplyLiteralColumns(
    List<Dictionary<string, object?>> data, List<LiteralColumn> literals)
{
    foreach (var row in data)
        foreach (var lit in literals)
            row[lit.Alias] = lit.Value;
}
```

Key-Reihenfolge ist hier zwangsläufig „Literale hinten" — die Gruppen-Row hat keine Select-Ordnung,
an der man sich orientieren könnte.

### `ExecuteAggregate`

Braucht nichts: der Parser lässt Select und Aggregate nicht gleichzeitig zu (`GetParser.cs:94`,
`if (aggregate is null && ...)`), die Liste kann dort nie ankommen.

## Interaktion mit anderen Klauseln

| Klausel | Verhalten | Warum |
|---------|-----------|-------|
| `where` | Literal-Aliase **nicht** referenzierbar | WHERE filtert vor der Projektion. Wie bei Computed Columns heute. |
| `distinct` | funktioniert | Literal ist zur Distinct-Zeit (`:255-266`) schon in der Row, trägt als Konstante nichts zum Key bei |
| `order by <literal-alias>` | erlaubt, No-Op | Konstante → `cmp == 0`. Whitelist nötig, s.o. |
| `count` | Literal irrelevant | `Data` ist leer (`:381-384`) |
| `page`/`size`/`after` | funktioniert | Cursor-Pfad geht über `ProjectPlaces` |
| Aggregate | nicht kombinierbar | Parser-seitig schon ausgeschlossen |
| `group by` | Literal pro Gruppen-Row | s.o. |
| Inner/Left/Right/Outer-Follow | Literal konstant auf allen Pfaden | Right/Outer-Fix s.o. |
| leeres Result | `[]`, kein Literal-Row | keine Rows → keine Projektion. Korrekt. |
| Follow-Target-Select | expliziter Fehler | s.o. |

### Einschränkung Key-Reihenfolge

Die Reihenfolge stimmt exakt im einfachen Pfad (Select + Literale, keine Extras, kein Follow).
Sobald `row.Remove` läuft — Computed-Extras (`:249`), Follow-Extras (`:304`), Follow-`_id`
(`:317`) — ist die Dictionary-Enumerationsreihenfolge ohnehin nicht mehr garantiert, weil ein
späterer Add den freigewordenen Slot wiederverwenden kann. Das ist **bestehendes Verhalten** und
betrifft Computed Columns heute genauso; Literale ändern nichts daran. Nicht als Garantie
dokumentieren.

## Tests

Parser:
- je ein Literal pro Typ (string/int/float/bool/null/negativ) mit Alias
- `select 1` / `select 'auto'` ohne Alias → `literal in select requires an alias`
- `select true` (ohne `as`) → weiterhin `UNKNOWN_COLUMN` auf Spalte `true` *(Regression!)*
- `select true as x` → Literal, nicht Spalte
- `select price -1 as x` → weiterhin Computed Column *(Regression!)*
- `-select 1 as x` → `literals are not allowed in '-select'`
- `select 1 as x` allein (ohne echte Spalte) → gültig
- gemischt: `select host, 1 as a, price * 2 as b, 'x' as c`
- `follow ... as b select 1 as x` → `literals are not allowed in a follow select`
- Duplikate: `select host, host` ok / `select host, 1 as host` Fehler /
  `select host as x, price as x` Fehler / `select price*2 as x, qty*3 as x` Fehler *(Breaking!)* /
  `-select host, host` ok

Executor:
- Werte + Typen kommen korrekt in der Response an
- **Key-Reihenfolge**: `select true as a, host` → `a` vor `host`
- **Right/Outer + Basis-Literal**: unmatched Target-Rows behalten den Literalwert *(der Bug oben)*
- Left-Join + Literal, Inner-Join + Literal
- Post-Follow-Select mit Literal, inkl. Reihenfolge
- Basis-Literal + expliziter Post-Follow-Select → Literal weg; mit Auflistung → bleibt
- `distinct` + Literal
- `order by <literal-alias>`
- Cursor-Paging (`after`) + Literal
- Top-N-Fast-Path (`order by _id limit N`) + Literal
- `group by` + Literal
- leeres Result → `[]`

## Doku-Sync

Bei Umsetzung **beide** Stellen anfassen — der Marketplace-Skill ist eine abgeleitete Kopie:

- `sproutdb-reference.md` — Select-Abschnitt
- Skill `references/query-language.md` (Zeile ~134 Grammatik, ~151 Beispiele)
- `todo.md` — Zeile in die "Implementiert"-Tabelle, neben "Computed Columns"
- Release-Notes: Duplikat-Prüfung ist **breaking** für bestehende Computed-Queries mit
  doppeltem Alias

## Aufwand

Grob 4-5h inkl. Tests.

- Parser: der größere Teil — Lookahead, 4 Call-Sites, Duplikat-Prüfung, Follow-Target-Fehler
- Projektion: klein, weil `ProjectionEntry`/`ResolveProjection` die Ordnung schon können —
  ein Enum-Member, ein Feld, ein Switch-Zweig, ein Merge, drei durchgereichte Parameter
- `ExecuteFollow`: ein Parameter, eine Bedingung
- Post-Follow-Projektion + `ExecuteGrouped`: je ein kleiner Block

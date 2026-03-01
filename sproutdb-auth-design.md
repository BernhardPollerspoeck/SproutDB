# SproutDB Auth & Permissions Design

## Grundprinzip

- Auth ist **opt-in** — ohne `AddSproutDBAuth()` ist alles erlaubt, kein Header nötig
- Aktivierung via `AddSproutDBAuth(o => o.MasterKey = "sdb_ak_...")`
- MasterKey = Bootstrap-Key, in Code konfiguriert
- ASP.NET Middleware handelt Auth komplett
- Alles lebt in `SproutDB.Core` (kein separates Projekt)

---

## API Key Format

Prefix: `sdb_ak_<32 random base62 chars>`

- Erkennbar in Logs/Code
- GitHub/GitLab Secret-Scanner können das Pattern matchen
- Beispiel: `sdb_ak_a3f2b7c9d1e4f6a8b0c2d4e6f8a1b3c5`

---

## Rollen

| Rolle | Rechte |
|---|---|
| `admin` | Alles: create/purge database/table, add/purge/rename column, upsert, delete, get, describe, grant/revoke/restrict |
| `writer` | upsert, delete, get, describe |
| `reader` | get, describe |

- **Keine globale Rolle** — Rolle wird immer pro Database vergeben
- Key ohne Grants hat keinen Zugriff

---

## Vererbung: Restrict-Only

Table-Permissions können die Database-Permission nur **einschränken**, nie erweitern.

Beispiel: Key hat `writer` auf `shop`
- `restrict orders to reader` -> orders ist read-only
- `restrict payments to none` -> payments komplett gesperrt
- Table ohne Restriction erbt Database-Rolle (`writer`)

---

## Query-Syntax

### Key-Management (nur MasterKey)

```
create apikey 'backend-service'
purge apikey 'backend-service'
rotate apikey 'backend-service'
```

### Grant / Revoke (MasterKey + admin auf jeweiliger DB)

```
grant writer on shop to 'backend-service'
grant reader on logs to 'backend-service'
grant admin on metrics to 'backend-service'
revoke shop from 'backend-service'
```

### Restrict / Unrestrict (MasterKey + admin auf jeweiliger DB)

```
restrict orders to reader for 'backend-service' on shop
restrict * to none for 'backend-service' on shop
unrestrict orders for 'backend-service' on shop
```

### Lesen (normales GET auf _system)

```
get _api_keys
get _api_keys where name = 'backend-service'
get _api_permissions where database = 'shop'
get _api_restrictions where key_name = 'backend-service'
```

---

## Ausführungsberechtigung

| Query | Wer darf |
|---|---|
| `create apikey` | MasterKey |
| `purge apikey` | MasterKey |
| `rotate apikey` | MasterKey |
| `grant` | MasterKey, admin auf der DB |
| `revoke` | MasterKey, admin auf der DB |
| `restrict` | MasterKey, admin auf der DB |
| `unrestrict` | MasterKey, admin auf der DB |

---

## System-Tabellen (`_system`)

### `_api_keys`

| Feld | Typ | Beschreibung |
|---|---|---|
| name | string | Eindeutiger Key-Name |
| key_prefix | string | Erste 8 Zeichen für Log-Identifikation |
| key_hash | string | SHA-256 Hash des Keys |
| created_at | datetime | Erstellungszeitpunkt |
| last_used_at | datetime | Letzte Verwendung |

### `_api_permissions`

| Feld | Typ | Beschreibung |
|---|---|---|
| key_name | string | FK auf _api_keys.name |
| database | string | Database-Name |
| role | string | admin / writer / reader |

### `_api_restrictions`

| Feld | Typ | Beschreibung |
|---|---|---|
| key_name | string | FK auf _api_keys.name |
| database | string | Database-Name |
| table | string | Table-Name (`*` = alle) |
| role | string | reader / none |

---

## HTTP Header

- `X-SproutDB-ApiKey` — Pflicht wenn Auth aktiv
- Fehlt der Header: `AUTH_REQUIRED` (401)
- Ungültiger Key: `AUTH_INVALID` (401)
- Keine Berechtigung: `PERMISSION_DENIED` (403)

---

## Error Codes

| Code | HTTP | Wann |
|---|---|---|
| `AUTH_REQUIRED` | 401 | Kein API Key mitgegeben |
| `AUTH_INVALID` | 401 | API Key unbekannt |
| `PERMISSION_DENIED` | 403 | Rolle hat keine Berechtigung |

---

## DI Setup

```csharp
builder.Services.AddSproutDB(options =>
{
    options.DataDirectory = "/data/sproutdb";
});

builder.Services.AddSproutDBAuth(options =>
{
    options.MasterKey = "sdb_ak_...";
});

app.MapSproutDB();
```

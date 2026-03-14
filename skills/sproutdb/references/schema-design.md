# SproutDB Schema Design Reference

## Data Types

### Numeric

| Type | Size | Range |
|------|------|-------|
| `sbyte` | 1B | -128 to 127 |
| `ubyte` | 1B | 0 to 255 |
| `sshort` | 2B | -32,768 to 32,767 |
| `ushort` | 2B | 0 to 65,535 |
| `sint` | 4B | ±2.1 billion |
| `uint` | 4B | 0 to 4.2 billion |
| `slong` | 8B | ±9.2 quintillion |
| `ulong` | 8B | 0 to 18.4 quintillion (ID type) |
| `float` | 4B | 32-bit IEEE 754 |
| `double` | 8B | 64-bit IEEE 754 |

### Text

| Type | Size |
|------|------|
| `string` | 255 bytes default, UTF-8, zero-padded |
| `string N` | N bytes (max 1,048,576) |

### Date/Time

| Type | Size | Format |
|------|------|--------|
| `date` | 4B | `yyyy-MM-dd` |
| `time` | 8B | `HH:mm:ss.ffff` |
| `datetime` | 8B | `yyyy-MM-dd HH:mm:ss.ffff` (UTC) |

### Other

| Type | Size | Notes |
|------|------|-------|
| `bool` | 1B | `true` / `false` |
| `blob` | 8B counter | Binary, stored as files on disk, Base64 in/out |

---

## _id Column

- Every table has automatic `_id` column (type: `ulong`)
- Auto-increment, monotonically increasing, never recycled
- Cannot be manually defined or set on insert
- `_id` in upsert body is NOT allowed for insert — only for update

## Nullable & Defaults

- Columns are **nullable by default**
- `default VALUE` makes column non-nullable and sets default on insert
- Defaults are NOT retroactively applied to existing rows

## strict Modifier

- Prevents type changes on the column
- Write with wider type → `STRICT_VIOLATION` error

## Naming Rules

- First char: ASCII letter (a-z, A-Z) or `_`
- Following: ASCII letter, digit (0-9), or `_`
- `_` prefix reserved for system tables
- Case-insensitive (stored lowercase)

---

## Relationship Patterns

SproutDB has **no foreign keys**. Relationships are resolved at query time via `follow`.

### 1:N

```
create table users (name string)
create table orders (user_id ulong, amount double)
create index orders.user_id

## Query
get users
    follow users._id -> orders.user_id as orders
```

### N:1 (Lookup)

```
create table orders (product_id ulong, quantity uint)
create table products (name string, price double)

## Query
get orders
    follow orders.product_id -> products._id as product
```

### M:N (Junction Table)

```
create table users (name string)
create table roles (name string)
create table user_roles (user_id ulong, role_id ulong)
create index user_roles.user_id
create index user_roles.role_id

## Query
get users
    follow users._id -> user_roles.user_id as ur
    follow ur.role_id -> roles._id as role
```

---

## Design Rules

### No Arrays/JSON — Use Junction Tables

```
## WRONG: storing as JSON string
## RIGHT:
create table user_roles (user_id ulong, role string 30)
create index user_roles.user_id
```

### Large Text / Binary → Blob

- Use `blob` for variable-length content (descriptions, changelogs, images)
- Blob = Base64 in/out, separate file on disk, NOT searchable
- Do NOT use `string 1048576` — fixed-size rows waste space for short text

### Enum Fields → String with Fixed Size

```
create table tickets (status string 20, priority string 10)
## Readable in queries: where status = 'open'
```

### Numeric Types — Size Generously

Type widening (e.g. `ubyte` → `ushort`) is currently **broken** (file rebuild not implemented).
Choose the right type from the start. String size can be changed via `alter column`.

### Index Strategy

- Index columns used in WHERE with equality/range filters
- Index foreign key columns used in follow
- Unique index for natural keys (email, slug, etc.)
- Blob columns CANNOT be indexed

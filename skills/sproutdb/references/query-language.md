# SproutDB Query Language Reference

## Literals

| Type | Syntax | Example |
|------|--------|---------|
| String | Single quotes | `'hello'`, `'it\'s'` |
| Integer | Digits | `42`, `-7` |
| Float | Digits with dot | `3.14`, `-0.5` |
| Boolean | Keyword | `true`, `false` |
| Null | Keyword | `null` (only in WHERE) |
| Date | String format | `'2025-01-15'` |
| DateTime | String format | `'2025-01-15 14:30:00.0000'` |
| Duration | Number + unit | `7d`, `24h`, `30m` |

## Comments

```
## This is a comment ##
get users ## inline comment ## where active = true
## Comment to end of line (closing ## optional)
```

---

## CREATE TABLE

```
create table NAME
create table NAME (col1 type [size] [strict] [default val], ...)
create table NAME (...) ttl DURATION
```

Examples:
```
create table users (name string, email string 320 strict, age ubyte, active bool default true)
create table sessions (token string 64 strict) ttl 24h
create table products (name string 200, price double default 0)
```

- No parentheses around type size: `string 320` NOT `string(320)`
- `default VALUE` makes column non-nullable
- `strict` prevents type widening on the column

---

## UPSERT (Insert & Update)

```
## Insert (new _id auto-generated)
upsert users {name: 'John', email: 'john@test.com', age: 25}

## Bulk Insert
upsert users [{name: 'John', age: 25}, {name: 'Jane', age: 30}]

## Update by _id (implicit on _id)
upsert users {_id: 1, name: 'John Updated'}

## Upsert by column (insert if not found, update if found)
upsert users {email: 'john@test.com', name: 'John'} on email

## Row-level TTL
upsert sessions {token: 'abc123', ttl: 24h}
```

Rules:
- No `_id` and no `on` → Insert (auto-generated ID)
- `_id` in body → implicit `on _id`, updates if exists
- `on COLUMN` → lookup by column value: update if found, insert if not
- `_id` can NEVER be manually set on insert
- TTL field `ttl: DURATION` sets row expiry (`0` = no TTL)
- Bulk limit: default 100 records per upsert

---

## GET

```
get TABLE
    [AGGREGATE column [as alias]]
    [select col1, col2 | -select col1, col2]
    [distinct]
    [where WHERE]
    [count]
    [group by col1, col2]
    [order by col1 [desc], col2 [asc]]
    [limit N]
    [page N size M]
    [follow FOLLOW]*
    [select col1, follow_alias.col2]
```

Examples:
```
get users
get users where active = true and age >= 18
get users select name, email where active = true
get users -select password_hash
get users order by name asc, created desc
get users page 2 size 20
get users where active = true limit 10
get users where active = true count

## Aggregation
get orders sum total as revenue where status = 'completed'
get orders avg total as average_order
get orders min created as first_order
get orders max total as biggest_order

## Group By
get orders sum total as revenue group by status

## Computed Columns
get products select name, price * quantity as line_total

## Distinct
get orders select status distinct
```

---

## DELETE

```
delete TABLE where WHERE
```

**WHERE is mandatory** — no accidental full-table deletes.

```
delete users where active = false
delete sessions where created < '2024-01-01 00:00:00.0000'
```

---

## DESCRIBE

```
describe            ## List all tables
describe TABLE      ## Show table schema
```

---

## Column Operations

```
add column TABLE.COLUMN TYPE [SIZE] [strict] [default VALUE]
rename column TABLE.OLD_NAME to NEW_NAME
alter column TABLE.COLUMN string NEW_SIZE
```

---

## Index Operations

```
create index TABLE.COLUMN
create unique index TABLE.COLUMN
purge index TABLE.COLUMN
```

- Unique index allows NULL values (only non-null must be unique)
- Blob columns cannot be indexed

---

## Purge Operations

```
purge table NAME
purge database
purge column TABLE.COLUMN
purge index TABLE.COLUMN
purge ttl TABLE
```

---

## TTL

```
create table sessions (token string) ttl 24h
upsert sessions {token: 'abc', ttl: 1h}
purge ttl sessions
```

Units: `m` (minutes), `h` (hours), `d` (days).

---

## Auth Commands

```
create apikey 'name'
purge apikey 'name'
rotate apikey 'name'
grant ROLE on DATABASE to 'apikey_name'
revoke DATABASE from 'apikey_name'
restrict TABLE to ROLE for 'apikey_name' on DATABASE
unrestrict TABLE for 'apikey_name' on DATABASE
```

Roles: `admin`, `writer`, `reader`. Restrict only `reader` or `none`.

---

## Backup & Restore

```
backup
restore 'path/to/backup'
```

---

## WHERE Clause

### Comparison

| Operator | Description |
|----------|-------------|
| `=` | Equal |
| `!=` | Not equal |
| `>`, `>=`, `<`, `<=` | Comparison |
| `contains` | Substring (string only) |
| `starts` | Prefix (string only) |
| `ends` | Suffix (string only) |
| `between X and Y` | Inclusive range |
| `not between X and Y` | Outside range |
| `in [v1, v2]` | Membership list |
| `not in [v1, v2]` | Exclusion list |

### Null checks

```
column is null
column is not null
```

### Membership

```
column in ['val1', 'val2', 'val3']
column not in ['val1', 'val2']
```

### Logic

| Operator | Precedence |
|----------|-----------|
| `or` | Lowest |
| `and` | Medium |
| `not` | Highest (prefix) |

String comparisons are **case-sensitive** (byte-level UTF-8).

---

## FOLLOW (Join)

### Arrow Types

| Arrow | Type | Behavior |
|-------|------|----------|
| `->` | Inner | Only rows with match in both tables |
| `->?` | Left | All source rows, NULL if no match |
| `?->` | Right | All target rows, NULL if no match |
| `?->?` | Outer | All rows from both tables |

### Syntax

```
follow SOURCE_TABLE.SOURCE_COL ARROW TARGET_TABLE.TARGET_COL as ALIAS
    [select col1, col2]
    [where CONDITION]
```

### Examples

```
## Inner join
get users
    follow users._id -> orders.user_id as orders

## Left join (all users, even without orders)
get users
    follow users._id ->? orders.user_id as orders

## Filter on followed table
get users
    follow users._id -> orders.user_id as orders
        where orders.total > 100

## Multiple conditions on follow filter
get users
    follow users._id -> orders.user_id as orders
        where orders.status in ['completed', 'shipped'] and orders.total > 50

## Follow filter with string operators
get users
    follow users._id -> orders.user_id as orders
        where orders.product starts 'Premium'

## Select on followed table
get users
    follow users._id -> orders.user_id as orders
        select product, total

## Chained follows
get users
    follow users._id -> orders.user_id as orders
    follow orders.product_id -> products._id as product

## Follow with filter + chained second follow
get users
    follow users._id -> orders.user_id as orders
        where orders.status = 'completed'
    follow orders.product_id -> products._id as product

## Post-follow select
get users
    follow users._id -> orders.user_id as orders
    select name, orders.total, orders.product
```

Follow expands rows: 1 user with 3 orders → 3 result rows.
Follow columns are prefixed with alias: `orders._id`, `orders.total`.

---

## Error Codes

| Code | Description |
|------|-------------|
| `SYNTAX_ERROR` | Query parse failure |
| `UNKNOWN_TABLE` | Table doesn't exist |
| `UNKNOWN_COLUMN` | Column doesn't exist |
| `TABLE_EXISTS` | Table already exists |
| `TYPE_MISMATCH` | Value doesn't match column type |
| `NOT_NULLABLE` | NULL for non-nullable column |
| `STRICT_VIOLATION` | Type widening on strict column |
| `BULK_LIMIT` | Too many records in bulk upsert |
| `WHERE_REQUIRED` | DELETE needs WHERE |
| `UNIQUE_VIOLATION` | Unique index violated |
| `PROTECTED_NAME` | Name with `_` prefix (system-reserved) |
| `AUTH_REQUIRED` | No API key provided |
| `PERMISSION_DENIED` | Insufficient permissions |

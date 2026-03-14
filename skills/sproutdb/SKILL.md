---
name: sproutdb
description: SproutDB query language, schema design, and C# integration reference. ALWAYS use this skill when writing ANY code that touches SproutDB — queries, migrations, table design, upserts, follows/joins, TTL, indexes, auth, or ISproutDatabase/ISproutServer usage. Also trigger when the user mentions SproutDB, sprout queries, or references SproutDB tables/namespaces. SproutDB has a custom query language that is NOT SQL — do not guess syntax, always consult this skill first. Even for simple queries, the syntax differs enough from SQL that checking is essential.
---

# SproutDB Reference Skill

SproutDB is a custom embedded/networked database with its own query language (NOT SQL).
**Never guess syntax — always check the reference.**

## When to read which file

| Task | Read |
|------|------|
| Writing queries (get, upsert, delete, create table, etc.) | [references/query-language.md](references/query-language.md) |
| Designing tables, choosing types, modeling relationships | [references/schema-design.md](references/schema-design.md) |
| C# integration (DI, migrations, ISproutDatabase, responses) | [references/csharp-integration.md](references/csharp-integration.md) |

## Critical differences from SQL

- **UPSERT, not INSERT/UPDATE** — `upsert table {key: 'val'}` with JSON-like body
- **GET, not SELECT** — `get table where ...` not `select * from table where ...`
- **FOLLOW, not JOIN** — `follow source.col -> target.col as alias`
- **No parentheses in column definitions** — `create table t (name string 100)` not `(name string(100))`
- **No semicolons** — queries are single-line or multi-line without terminators
- **Comments use `##`** — not `--` or `/* */`
- **String literals use single quotes** — `'hello'` not `"hello"`
- **_id is automatic** — ulong, auto-increment, cannot be set manually on insert
- **No ALTER TABLE** — use `add column`, `rename column`, `alter column` as separate commands
- **DELETE requires WHERE** — no way to delete all rows in one statement
- **No foreign keys** — relationships via follow at query time only
- **No arrays/JSON** — use junction tables instead

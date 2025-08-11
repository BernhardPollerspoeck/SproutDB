# CREATE INDEX

## Description

The CREATE INDEX command improves query performance in SproutDB by creating an index on specified table fields. It enables faster data retrieval for queries that filter, sort, or join on the indexed fields. This command allows for manual optimization of database performance based on specific query patterns.

## Where It Applies

- Used as a standalone command outside of data queries
- Specifies a table and one or more fields to index
- Requires a unique index name for reference
- Requires appropriate permissions (schema operation permission)
- Can index any field or combination of fields

## What It Does

- Creates a data structure that allows for faster lookups on the indexed fields
- Improves query performance for operations involving the indexed fields
- Speeds up WHERE conditions, ORDER BY clauses, and JOINs using the indexed fields
- Creates a schema change record in the branch history
- Index is automatically maintained when data changes
- May slightly slow down write operations (inserts/updates) to maintain the index

## Examples

### Single field index

```sql
create index users_email on users (email)
```

### Composite (multi-field) index

```sql
create index users_age_city on users (age, city)
```

### Index for optimizing specific queries

```sql
create index products_category_price on products (category, price)
```

### Index for join optimization

```sql
create index orders_user_id on orders (user_id)
```

### Index on nested field

```sql
create index users_address_country on users (address.country)
```

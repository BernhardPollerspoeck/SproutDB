# DROP INDEX

## Description

The DROP INDEX command removes an existing index from a table in SproutDB. It eliminates indexes that are no longer needed or are not providing performance benefits. This command helps manage database resources by removing unnecessary indexes that consume storage space and slow down write operations.

## Where It Applies

- Used as a standalone command outside of data queries
- References a specific index by name
- Requires appropriate permissions (schema operation permission)
- Can only be used on existing indexes
- Cannot be undone (index must be recreated if needed again)

## What It Does

- Permanently removes the specified index from the database
- Frees up storage space used by the index
- Creates a schema change record in the branch history
- May slightly improve write performance by eliminating index maintenance
- May slow down queries that were benefiting from the index
- Does not affect the data in the table, only the performance characteristics

## Examples

### Basic index removal

```sql
drop index users_old_index
```

### Removing an index that's no longer beneficial

```sql
drop index products_unused_field_index
```

### Replacing an index with a better one (two commands)

```sql
create index users_new_email_index on users (email, status)
drop index users_email_index
```

### Cleaning up obsolete indexes

```sql
drop index orders_legacy_date_index
```

### Removing an index on a field that was purged

```sql
drop index users_purged_field_index
```

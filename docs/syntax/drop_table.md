# DROP TABLE

## Description

The DROP TABLE command permanently removes a table and all its data from the SproutDB database. This operation deletes the table structure, all contained records, and associated indexes. Use this command with caution as it permanently removes data and cannot be easily recovered except through branch history.

## Where It Applies

- Used as a standalone command outside of data queries
- References a specific existing table
- Requires appropriate permissions (schema operation permission)
- Cannot be undone (table must be recreated if needed again)
- Cannot drop system tables

## What It Does

- Permanently removes the specified table from the database
- Deletes all data stored in the table
- Removes all indexes associated with the table
- Creates a schema change record in the branch history
- Frees up storage space used by the table and its data
- Prevents any further operations on the dropped table

## Examples

### Basic table removal

```sql
drop table old_users
```

### Drop table with caution

```sql
drop table customers
```

### Drop temporary or test table

```sql
drop table temp_import_data
```

### Drop table and create replacement (two commands)

```sql
create table new_products
drop table old_products
```

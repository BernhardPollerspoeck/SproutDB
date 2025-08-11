# ADD COLUMN

## Description

The ADD COLUMN command creates or restores a field in a table schema in SproutDB. It allows for explicit schema modification by adding new fields or re-enabling previously purged fields. This command provides controlled schema evolution alongside the automatic schema adaptation that occurs during data operations.

## Where It Applies

- Used as a standalone command outside of data queries
- References a specific table and field name
- Includes a data type specification
- Creates or modifies schema only, does not insert data
- Requires appropriate permissions (schema operation permission)

## What It Does

- If the field does not exist: Creates a new field with the specified type
- If the field was purged: Restores it with the more flexible of the original and specified types
- If the field exists and is active: May expand its type if needed to accommodate the specified type
- Creates a schema change record in the branch history
- Makes the field immediately available for use in data operations
- Does not affect existing data in the table

## Examples

### Adding a new field

```sql
add column users.premium boolean
```

### Adding a field with a specific type

```sql
add column products.dimensions object
```

### Restoring a previously purged field

```sql
add column users.old_field string
```

### Adding a field with a mixed type

```sql
add column products.sku mixed
```

### Adding a field to multiple tables (multiple commands)

```sql
add column customers.verified boolean
add column suppliers.verified boolean
```

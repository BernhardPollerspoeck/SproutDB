# PURGE COLUMN

## Description

The PURGE COLUMN command hides a field in a table schema in SproutDB without permanently deleting it. It removes the field from active use while preserving the ability to restore it later if needed. This command provides a safer alternative to irreversible schema deletions, maintaining compatibility with historical data.

## Where It Applies

- Used as a standalone command outside of data queries
- References a specific table and field name
- Requires appropriate permissions (schema operation permission)
- Can be applied to any field except the required 'id' field
- Can be reversed using the ADD COLUMN command

## What It Does

- Marks the specified field as hidden/inactive
- Prevents the field from appearing in queries and results
- Preserves the field's data and type information for historical purposes
- Creates a schema change record in the branch history
- Does not physically delete data (historical queries can still access it)
- Makes the field unavailable for use in new data operations until restored

## Examples

### Basic column purge

```sql
purge column users.old_field
```

### Purging a field that's no longer needed

```sql
purge column products.legacy_code
```

### Purging personally identifiable information

```sql
purge column users.social_security_number
```

### Purging deprecated fields

```sql
purge column orders.old_pricing_model
```

### Purging multiple fields (multiple commands)

```sql
purge column logs.debug_info
purge column logs.stack_trace
```

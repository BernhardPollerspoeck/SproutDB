# RESPAWN

## Description

The RESPAWN command creates a clean export of a branch as a new database instance in SproutDB. It generates a fresh database with the current state of data but without any history, providing a way to start with a clean slate while preserving the original database. This feature is particularly valuable for compliance, data purging, and creating customer exports.

## Where It Applies

- Used as a standalone command outside of data queries
- Can be applied to any branch in the database
- Requires appropriate permissions (admin permission)
- Can include an optional date cutoff point

## What It Does

- Creates an entirely new, separate database instance
- Copies the current state of data from the specified branch
- Removes all history, leaving only the current state as an "Initial Commit"
- Preserves the original database and branch intact
- Can optionally include only data from a specific date forward
- Creates a new database with its own independent branches and permissions

## Examples

### Basic respawn of a branch

```sql
respawn branch main as new-clean-database
```

### Respawn with date cutoff

```sql
respawn branch main as backup-2024 since "2024-01-01"
```

### Creating a customer export

```sql
respawn branch customer-a/prod as customer-a-export
```

### Clean start after development

```sql
respawn branch feature/cleanup as fresh-start
```

### GDPR-compliant data export

```sql
respawn branch customer/acme-corp/data as gdpr-export since "2023-06-15"
```

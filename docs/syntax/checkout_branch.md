# CHECKOUT BRANCH

## Description

The CHECKOUT BRANCH command switches the active branch in a SproutDB session. It changes which branch subsequent commands will operate on without modifying any data. This command allows developers and applications to easily move between different versions of the database to work with isolated feature sets or environments.

## Where It Applies

- Used as a standalone command outside of data queries
- Can reference any existing branch in the database
- Affects only the current connection/session
- Requires appropriate permissions for the target branch

## What It Does

- Changes the active branch for the current connection
- Makes all subsequent commands operate on the specified branch
- Does not modify any data in either the previous or new active branch
- Preserves any uncommitted changes in the previous branch
- Automatically loads the schema and data state of the target branch
- Returns an error if the specified branch does not exist

## Examples

### Basic branch checkout

```sql
checkout branch main
```

### Checking out a feature branch

```sql
checkout branch feature/new-pricing
```

### Checking out a customer-specific branch

```sql
checkout branch customer/acme-corp/production
```

### Switching to a testing branch

```sql
checkout branch test/performance-improvements
```

### Checking out a historical branch

```sql
checkout branch archive/2023-backup
```

# CREATE BRANCH

## Description

The CREATE BRANCH keyword initiates a new version branch in SproutDB. It establishes a separate line of development based on an existing branch, allowing for isolated changes without affecting the source branch. This feature enables parallel development, experimentation, and feature isolation within the database.

## Where It Applies

- Used as a standalone command outside of data queries
- Can be used to create branches from any existing branch
- Requires branch naming that follows the branch naming convention
- Requires appropriate permissions (branch operation permission)

## What It Does

- Creates a new branch starting from the current state of the specified source branch
- Makes an exact copy of all data and schema at the point of branching
- Does not affect the source branch in any way
- Allows for independent modifications in the new branch
- Creates an initial commit in the branch history
- New branch becomes available immediately for read and write operations

## Examples

### Basic branch creation

```sql
create branch feature/new-pricing from main
```

### Creating a branch for customer-specific development

```sql
create branch customer/acme-corp/custom-features from main
```

### Creating a branch for testing

```sql
create branch test/performance-improvements from develop
```

### Creating a branch for bug fixing

```sql
create branch bugfix/order-calculation from main
```

### Creating a backup branch before major changes

```sql
create branch backup/pre-migration-2024 from production
```

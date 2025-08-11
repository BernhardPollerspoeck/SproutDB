# MERGE

## Description

The MERGE command combines changes from one branch into another in SproutDB. It integrates modifications made in a source branch into a target branch using deterministic merge strategies. This feature allows for consolidating development work, incorporating features, and maintaining consistent data across different branches.

## Where It Applies

- Used as a standalone command outside of data queries
- Requires specifying both source and target branches
- Target branch must be the currently checked out branch
- Requires appropriate permissions (merge operation permission)
- Can merge between any branches with a common ancestor

## What It Does

- Integrates all changes from the source branch into the target branch
- Uses deterministic row-level merging strategies to resolve conflicts
- Automatically merges schema changes using type evolution rules
- Creates a new commit in the target branch's history recording the merge
- Preserves both branches after the operation (source branch remains intact)
- Uses "newest timestamp wins" for conflicts on the same field

## Examples

### Basic merge

```sql
merge feature/new-pricing into main
```

### Merging bug fixes

```sql
merge bugfix/order-calculation into develop
```

### Merging customer customizations back to main

```sql
merge customer/acme-corp/custom-features into main
```

### Merging development branch into production

```sql
merge develop into production
```

### Merge with automatic conflict resolution

```sql
merge test/schema-updates into main
```

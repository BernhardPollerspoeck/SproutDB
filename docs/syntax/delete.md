# DELETE

## Description

The DELETE keyword is used to remove records from tables in SproutDB. It allows for targeted removal of data based on specified conditions. DELETE operations permanently remove data from the current branch, but the versioning system ensures historical access to deleted data through time travel queries.

## Where It Applies

- Used as the first keyword in a data removal command
- Can be used on any table in the database
- Must include a WHERE clause to specify which records to delete
- Can use time-based conditions for efficient deletion of old records

## What It Does

- Permanently removes records matching the specified conditions from the current branch
- Creates a new commit in the version history recording the deletion
- Does not physically remove data from history (previous commits still contain the data)
- Returns the number of records deleted

## Examples

### Basic deletion with condition

```sql
delete users where age < 18
```

### Time-based deletion

```sql
delete users where last_login before 1 year
```

### Complex condition deletion

```sql
delete orders where status = "cancelled" and date < "2024-01-01"
```

### Deletion based on related data

```sql
delete products where category = "discontinued" and orders.count = 0
```

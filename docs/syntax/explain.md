# EXPLAIN

## Description

The EXPLAIN command analyzes query execution plans in SproutDB to identify performance characteristics and optimization opportunities. It provides detailed insights into how the database processes a query, including which indexes are used and where performance bottlenecks might occur. This diagnostic tool helps developers and administrators optimize their queries for better performance.

## Where It Applies

- Used as a prefix to a GET query
- Can be applied to any valid query
- Does not execute the query or return data results
- Requires appropriate permissions (same as executing the actual query)
- Particularly useful for complex queries with multiple joins or large datasets

## What It Does

- Analyzes how the query will be executed without actually running it
- Shows the sequence of operations the database will perform
- Identifies which indexes will be used (if any)
- Provides time estimates for each operation
- Suggests potential indexes that could improve performance
- Identifies unused indexes that could be dropped
- Helps diagnose slow queries before they impact the system

## Examples

### Explaining a simple query

```sql
explain get users where email = "john@test.com"
```

### Explaining a query with conditions

```sql
explain get users where email = "john@test.com" and age > 25
```

### Explaining a query with joins

```sql
explain get users 
  follow users.id -> orders.user_id as orders
  where orders.total > 500
```

### Explaining a complex query

```sql
explain get users 
  follow users.id -> orders.user_id as orders
  follow orders.id -> order_items.order_id as items
  where orders.status = "completed" and items.quantity > 5
  group by users.city
  having count > 10
  order by sum(orders.total) desc
```

### Sample output

```text
Query Plan:
1. Table Scan: users (SLOW - 50ms, 10k rows scanned)
   → Suggestion: create index users_email_age on users (email, age)
   
2. Filter: age > 25 (2ms)
3. Total: 52ms

Recommendations:
- Index missing for email column (would reduce scan to 1ms)
- Consider composite index for email+age filters
- Unused indexes: users_old_name_index
```

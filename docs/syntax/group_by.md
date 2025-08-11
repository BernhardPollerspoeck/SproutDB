# GROUP BY

## Description

The GROUP BY keyword organizes query results into groups based on specified fields in SproutDB. It enables aggregate operations by grouping records with the same values in the specified fields. This feature is fundamental for data analysis, reporting, and summarizing information across multiple records.

## Where It Applies

- Used in GET queries after the WHERE clause (if present)
- Can reference any field from the primary table or joined tables
- Often used in conjunction with aggregate functions
- Can be followed by a HAVING clause to filter groups
- Can be followed by an ORDER BY clause to sort groups

## What It Does

- Creates groups of records based on unique combinations of values in the specified fields
- Collapses multiple records into a single result row per group
- Makes aggregate functions (count, sum, avg, min, max) operate on each group separately
- Requires that all selected fields either be grouping fields or aggregate expressions
- Generates a result set with one row per unique group

## Examples

### Basic grouping

```sql
get users group by city
```

### Grouping with aggregation

```sql
get orders group by user_id select user_id, count(*), sum(total)
```

### Multiple grouping fields

```sql
get products group by category, supplier select category, supplier, avg(price)
```

### Filtering groups with HAVING

```sql
get users 
  group by city 
  having count > 10
  order by count desc
```

### Complex grouping with calculations

```sql
get orders 
  where status = "completed" 
  group by user_id, date_format(created, "%Y-%m") 
  select user_id, date_format(created, "%Y-%m") as month, sum(total) as total_spent
  having total_spent > 1000
  order by total_spent desc
```

### Grouping with joins

```sql
get users 
  follow users.id -> orders.user_id as orders
  group by users.city
  select users.city, avg(orders.total) as avg_order
  having count(orders.id) > 5
```

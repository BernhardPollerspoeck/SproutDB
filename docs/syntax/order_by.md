# ORDER BY

## Description

The ORDER BY keyword sorts query results based on specified fields in SproutDB. It determines the sequence in which records appear in the result set, allowing for both ascending and descending order. This keyword is crucial for presenting data in a meaningful, ordered way to facilitate analysis and readability.

## Where It Applies

- Used in GET queries, typically as one of the final clauses
- Placed after WHERE, GROUP BY, and HAVING clauses (if present)
- Can reference fields from primary table, joined tables, or calculated expressions
- Can sort by multiple fields with different sort directions
- Can sort by aggregate functions when used with GROUP BY

## What It Does

- Sorts the result set based on the values in the specified fields
- Orders records in ascending order by default
- Can use DESC keyword to sort in descending order
- Supports multi-level sorting (secondary sort when primary sort values match)
- Performs sorting as the final step before returning results
- NULL values are typically sorted first in ascending order

## Examples

### Basic ascending sort

```sql
get products order by price
```

### Descending sort

```sql
get products order by price desc
```

### Multi-field sorting

```sql
get users order by last_name asc, first_name asc
```

### Mixed direction sorting

```sql
get products order by category asc, price desc
```

### Sorting by expression or calculation

```sql
get products order by price * discount desc
```

### Sorting with GROUP BY and aggregates

```sql
get orders 
  group by user_id 
  order by sum(total) desc
```

### Complex sorting with joins

```sql
get users 
  follow users.id -> orders.user_id as orders
  select users.name, count(orders.id) as order_count
  order by order_count desc, users.name asc
```

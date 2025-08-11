# HAVING

## Description

The HAVING keyword filters grouped results in SproutDB queries based on aggregate conditions. It acts like a WHERE clause but operates on groups after they've been formed by a GROUP BY clause. This keyword is essential for filtering aggregated data without having to retrieve all groups first.

## Where It Applies

- Used in GET queries that include a GROUP BY clause
- Placed after the GROUP BY clause
- Can reference aggregate functions and grouping fields
- Cannot be used without a GROUP BY clause
- Processed after grouping and aggregation are completed

## What It Does

- Filters groups based on conditions involving aggregate functions
- Eliminates groups that don't meet the specified conditions
- Can use comparison operators with aggregate results
- Can combine multiple conditions with AND/OR
- Reduces the number of groups returned in the result set

## Examples

### Basic usage with count

```sql
get users 
  group by city 
  having count > 10
```

### Using aggregate functions in conditions

```sql
get orders 
  group by user_id 
  having sum(total) > 1000
```

### Multiple conditions

```sql
get products 
  group by category 
  having avg(price) > 50 and count > 5
```

### With complex expressions

```sql
get orders 
  group by date_format(created, "%Y-%m")
  having sum(total) > 10000 and max(total) < 5000
```

### Combined with other clauses

```sql
get users 
  where active = true
  group by city 
  having count > 10
  order by avg(age) desc
```

### With aggregate and non-aggregate conditions

```sql
get users 
  follow users.id -> orders.user_id as orders
  group by users.signup_source
  having count > 100 and users.signup_source != "unknown"
```

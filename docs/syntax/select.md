# SELECT

## Description

The SELECT keyword specifies which fields to include in the query results in SproutDB. It allows for precise control over the data returned, enabling projection of specific fields rather than retrieving entire records. SELECT is positioned at the end of the query to emphasize the logical flow from data source to final result.

## Where It Applies

- Used in GET queries, typically as the final clause
- Placed after all other clauses (WHERE, GROUP BY, HAVING, ORDER BY)
- Can reference fields from primary table or joined tables
- Can include aggregate functions
- Can include calculated expressions and literals

## What It Does

- Limits the query result to only the specified fields
- Returns all fields if no SELECT clause is provided
- Allows renaming fields with the "as" keyword
- Can perform calculations or transformations on field values
- Can combine fields from multiple joined tables
- Must include only grouping fields or aggregate functions when used with GROUP BY

## Examples

### Basic field selection

```sql
get users select name, email, age
```

### Selection with field aliases

```sql
get users select name as full_name, email as contact_email
```

### Selection from joined tables

```sql
get users 
  follow users.id -> orders.user_id as orders
  select users.name, orders.total, orders.date
```

### Selection with expressions

```sql
get products select name, price, price * 0.8 as sale_price
```

### Selection with aggregates

```sql
get orders 
  group by user_id
  select user_id, count(*) as order_count, sum(total) as total_spent
```

### Complex selection with multiple joins

```sql
get users 
  follow users.id -> orders.user_id as orders
  follow orders.id -> order_items.order_id as items
  follow items.product_id -> products.id as products
  where orders.status = "completed"
  select users.name, 
         products.name as product_name, 
         items.quantity, 
         items.price * items.quantity as item_total
```

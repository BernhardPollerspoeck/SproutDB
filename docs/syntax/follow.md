# FOLLOW

## Description

The FOLLOW keyword creates relationships between tables in SproutDB queries. It establishes a join path using a clear, intuitive syntax that connects related data through matching fields. This keyword is essential for constructing multi-table queries that retrieve related information in a single operation.

## Where It Applies

- Used within a GET query after specifying the primary table
- Can be chained multiple times to connect multiple tables
- Must include table fields, join direction, and alias
- Can specify join type (inner, left, right)

## What It Does

- Creates a join relationship between two tables
- Requires an alias for the joined table to enable clear referencing
- Matches records based on the specified field relationship
- Controls how unmatched records are handled based on join type
- Makes fields from the joined table available in the query

## Examples

### Basic inner join

```sql
get users 
  follow users.id -> orders.user_id as orders
  select users.name, orders.total
```

### Multiple joins (join chain)

```sql
get users 
  follow users.id -> orders.user_id as orders
  follow orders.id -> order_items.order_id as items
  follow items.product_id -> products.id as products
  where orders.total > 500
  select users.name, products.title, items.quantity
```

### Left join (keep unmatched records from left table)

```sql
get users 
  follow users.id -> orders.user_id as orders (left)
  where orders.id is null
  select users.name
```

### Right join (keep unmatched records from right table)

```sql
get users 
  follow users.id -> orders.user_id as orders (right)
  select users.name, orders.total
```

### Mixed join types

```sql
get users 
  follow users.id -> orders.user_id as orders (left)
  follow orders.id -> shipments.order_id as shipments (inner)
  select users.name, orders.total, shipments.tracking_number
```

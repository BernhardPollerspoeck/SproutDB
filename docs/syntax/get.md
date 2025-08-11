# GET

## Description

The GET keyword is used to retrieve data from tables in SproutDB. It serves as the primary query command for selecting records from the database, allowing for various filtering, joining, and projection options. GET queries form the foundation for data retrieval operations in the system.

## Where It Applies

- Used as the first keyword in a data retrieval query
- Can be used on any table in the database
- Can be used with or without additional clauses
- Can be used on any branch or at any point in time

## What It Does

- Instructs the database to retrieve records from the specified table
- Returns all fields from the table unless specified otherwise
- Returns all records that match the given conditions if WHERE clause is provided
- Returns records in default order unless ORDER BY is specified

## Examples

### Basic usage
```
get users
```

### With conditions
```
get users where age > 25
```

### With ordering
```
get products order by price desc
```

### With time-based filtering
```
get orders where date last 7 days
```

### With specific fields (projection)
```
get users.name, users.email where city = "Berlin"
```

### With joins
```
get users 
  follow users.id -> orders.user_id as orders
  where orders.total > 500
  select users.name, orders.total
```

### With grouping and aggregation
```
get users 
  where orders.total > 1000 
  group by city 
  having count > 10
  order by avg(age) desc
```

### With time travel
```
get products as of "2024-01-15"
```

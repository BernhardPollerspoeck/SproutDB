# WHERE

## Description

The WHERE keyword defines conditions that filter records in SproutDB queries. It allows for precise data retrieval by specifying criteria that records must meet to be included in the results. WHERE clauses support a wide range of operators and can filter on any field, including nested data structures.

## Where It Applies

- Used in GET queries to filter retrieved records
- Used in DELETE queries to specify which records to remove
- Placed after the table specification or JOIN clauses
- Can reference fields from the primary table or joined tables
- Can be combined with other clauses like GROUP BY, HAVING, and ORDER BY

## What It Does

- Filters records based on the specified conditions
- Supports comparison operators (<, >, =, !=, >=, <=)
- Supports logical operators (and, or)
- Supports special operators for collections (contains, in)
- Supports time-based filtering with special syntax
- Can access nested fields and array elements

## Examples

### Basic comparison

```sql
get users where age > 25
```

### Combining conditions with AND/OR

```sql
get users where age > 25 and city = "Berlin"
get products where price < 100 or category = "sale"
```

### Time-based filtering

```sql
get orders where date last 7 days
get users where last_login this month
get transactions where date > "2024-01-01"
```

### Working with collections

```sql
get products where category in ["tech", "books"]
get users where skills contains "Python"
```

### Nested data structures

```sql
get users where address.country = "Germany"
get posts where comments.any(author = "john" and upvotes > 5)
```

### Null checks

```sql
get users where profile is null
get orders where tracking_number is not null
```

### Complex nested conditions

```sql
get products where 
  (category = "electronics" and price > 500) or 
  (category = "books" and author = "John Smith")
```

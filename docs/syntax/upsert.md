# UPSERT

## Description

The UPSERT keyword is used to insert or update records in SproutDB tables. It provides a streamlined approach to data manipulation by automatically determining whether to create a new record or update an existing one. This keyword is fundamental for maintaining data in the database with minimal complexity.

## Where It Applies

- Used as the first keyword in a data manipulation command
- Can be used on any table in the database
- Works on one table at a time (no joins in upsert operations)
- Can include an optional "on" clause to specify the field for matching records

## What It Does

- If no matching record is found, creates a new record with the provided data
- If a matching record is found, updates that record with the new data
- When no "on" clause is provided, matches on the "id" field
- Generates a new id automatically if one is not provided in a new record
- Can operate on a single record or multiple records (bulk upsert)

## Examples

### Minimal insertion

```sql
upsert users {}
```

### Standard insertion with fields

```sql
upsert users {name: "John", age: 25}
```

### Update using explicit matching field

```sql
upsert users {email: "john@test.com", name: "John"} on email
```

### Update an existing record by ID

```sql
upsert users {id: 123, name: "John", age: 26}
```

### Bulk upsert

```sql
upsert users [
  {name: "John", age: 25},
  {name: "Jane", age: 30},
  {name: "Bob", age: 35}
] on email
```

### Complex object insertion

```sql
upsert users {
  name: "John", 
  profile: {
    settings: {theme: "dark", notifications: true},
    preferences: ["email", "sms"]
  }
}
```

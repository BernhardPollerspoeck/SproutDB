# SUBSCRIBE

## Description

The SUBSCRIBE command establishes real-time data change notifications in SproutDB. It creates a persistent connection that delivers events when data changes match the specified criteria. This feature enables building reactive applications, live dashboards, and data synchronization solutions with minimal polling overhead.

## Where It Applies

- Used as a standalone command outside of data queries
- Can target specific branches, tables, and operation types
- Requires a SignalR connection
- Requires appropriate read permissions for the specified branch and tables
- Can filter notifications by table name and operation type

## What It Does

- Establishes a subscription for real-time data change events
- Returns events when matching data changes occur
- Delivers notifications with complete information about the change
- Continues sending events until explicitly unsubscribed
- Maintains a persistent connection using SignalR
- Operates with minimal overhead on the server
- Delivers events in the same format as query responses

## Examples

### Subscribe to all changes on a branch

```sql
subscribe to branch main
```

### Subscribe to specific table changes

```sql
subscribe to branch main where table = "products"
```

### Subscribe to multiple tables

```sql
subscribe to branch main where table in ["users", "orders"]
```

### Subscribe to specific operation types

```sql
subscribe to branch main where operation = "upsert"
```

### Complex subscription filter

```sql
subscribe to branch main where table in ["users", "orders"] and operation = "upsert"
```

### Sample event message

```json
{
  "branch": "main",
  "table": "users", 
  "operation": "upsert",
  "data": [{
    "id": 123,
    "name": "John", 
    "age": 26
  }],
  "timestamp": "2024-08-11T14:30:00Z",
  "commit": "abc123"
}
```

# UNSUBSCRIBE

## Description

The UNSUBSCRIBE command terminates real-time data change notifications in SproutDB. It stops the delivery of events for previously established subscriptions, allowing clients to control when they receive notifications. This command helps manage resources and network traffic by enabling precise subscription lifecycle management.

## Where It Applies

- Used as a standalone command outside of data queries
- Can target specific branch subscriptions or all subscriptions
- Only affects subscriptions created by the current connection
- Does not require special permissions beyond the original subscription permissions
- Has immediate effect when executed

## What It Does

- Terminates a specific branch subscription or all active subscriptions
- Stops the delivery of real-time data change events
- Releases server resources associated with maintaining the subscription
- Does not affect other connections or their subscriptions
- Returns a confirmation of successful unsubscription
- Requires a new SUBSCRIBE command to restart notifications

## Examples

### Unsubscribe from a specific branch

```sql
unsubscribe from branch main
```

### Unsubscribe from all branches

```sql
unsubscribe all
```

### Unsubscribe from one branch and subscribe to another

```sql
unsubscribe from branch develop
subscribe to branch feature/new-feature
```

### Temporary unsubscribe during maintenance

```sql
// During high load or maintenance
unsubscribe all

// Later, when ready to resume
subscribe to branch main where table in ["users", "orders"]
```

### Sample unsubscribe response

```json
{
  "success": true,
  "message": "Unsubscribed from branch: main",
  "timestamp": "2024-08-11T15:45:23Z"
}
```

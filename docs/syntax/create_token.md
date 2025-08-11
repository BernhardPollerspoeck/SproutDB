# CREATE TOKEN

## Description

The CREATE TOKEN command generates a new Personal Access Token (PAT) in SproutDB with specific permissions. It establishes secure authentication credentials for applications and users to access the database with precisely controlled privileges. This command is essential for implementing the principle of least privilege in database access control.

## Where It Applies

- Used as a standalone command outside of data queries
- Requires admin permission to execute
- Specifies a unique token name for identification
- Includes detailed permission definitions
- Creates tokens at the database level (not server-level)

## What It Does

- Generates a new secure token string for authentication
- Associates the token with the specified permissions
- Creates an entry in the meta-database storing the token configuration
- Returns the generated token value (shown only once at creation)
- Makes the token immediately available for authentication
- Tokens cannot have more permissions than the creating admin

## Examples

### Creating a token with basic permissions

```sql
create token "dev-team" with {
  operations: [read, write],
  tables: [users, products],
  branches: [normal]
}
```

### Creating a token with admin privileges

```sql
create token "admin-ops" with {
  operations: [read, write, schema, branch, merge, admin],
  tables: [*],
  branches: [normal, protected]
}
```

### Creating a read-only token

```sql
create token "reporting-tool" with {
  operations: [read],
  tables: [orders, products, analytics],
  branches: [protected]
}
```

### Creating a token with limited table access

```sql
create token "inventory-app" with {
  operations: [read, write],
  tables: [products, inventory],
  branches: [normal]
}
```

### Sample response

```json
{
  "success": true,
  "token": "pat_abc123def456789",
  "name": "dev-team",
  "created": "2024-08-11T16:30:00Z",
  "permissions": {
    "operations": ["read", "write"],
    "tables": ["users", "products"],
    "branches": ["normal"]
  },
  "message": "Token created successfully. Save this token - it won't be shown again."
}
```

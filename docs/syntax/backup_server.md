# BACKUP SERVER

## Description

The BACKUP SERVER command creates a comprehensive backup of the SproutDB server configuration and metadata. Unlike database backups which focus on individual databases, this command backs up server-level components including global PATs, users, and settings. This feature is crucial for disaster recovery planning at the server infrastructure level.

## Where It Applies

- Used as a standalone command outside of data queries
- Requires server-level admin permission to execute
- Specifies a server backup file path
- Operates on the server infrastructure (not individual databases)
- Should be performed regularly as part of infrastructure maintenance

## What It Does

- Creates a complete backup file of the server configuration
- Includes the server-meta database (global PATs, users, settings)
- Preserves server-level configurations and permissions
- Stores the server backup file on the server's filesystem
- Returns metadata about the backup operation
- Does NOT include individual databases (those must be backed up separately)
- Can be used with a corresponding restore command to recover server configuration

## Examples

### Basic server backup

```sql
backup server to "server-backup-2024-08-11.gitdb"
```

### Backup with date in filename

```sql
backup server to "sproutdb-server-backup-2024-08-11.gitdb"
```

### Regular scheduled backup

```sql
backup server to "weekly-server-backup-2024-w32.gitdb"
```

### Pre-upgrade backup

```sql
backup server to "pre-upgrade-server-backup.gitdb"
```

### Sample response

```json
{
  "id": "req-456",
  "success": true,
  "backup_file": "server-backup-2024-08-11.gitdb",
  "file_size": "156MB",
  "location": "/var/backups/sproutdb-server/",
  "total_global_tokens": 25,
  "total_users": 12,
  "total_settings": 47,
  "timestamp": "2024-08-11T18:15:00Z",
  "duration_ms": 3217,
  "note": "Server backup does NOT include individual databases"
}
```

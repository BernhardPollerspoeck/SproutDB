# BACKUP DATABASE

## Description

The BACKUP DATABASE command creates a complete snapshot of a SproutDB database for safekeeping. It generates a comprehensive backup file that includes all branches, commits, schema evolution history, and configurations. This command provides crucial disaster recovery capabilities and enables database state preservation for archival purposes.

## Where It Applies

- Used as a standalone command outside of data queries
- Requires admin permission to execute
- Specifies a backup file path on the server
- Operates on the entire database (not branch-specific)
- Should be scheduled regularly for data protection

## What It Does

- Creates a complete backup file of the entire database
- Includes all branches and their full commit history
- Preserves schema evolution history across all branches
- Stores all admin/token configurations from the meta-database
- Writes the backup file to the server's filesystem
- Returns metadata about the backup operation
- Can be used with RESTORE DATABASE to recover the database

## Examples

### Basic database backup

```sql
backup database to "backup-2024-08-11.gitdb"
```

### Backup with date in filename

```sql
backup database to "mydb-backup-2024-08-11.gitdb"
```

### Regular scheduled backup

```sql
backup database to "weekly-backup-2024-w32.gitdb"
```

### Pre-migration backup

```sql
backup database to "pre-migration-backup.gitdb"
```

### Sample response

```json
{
  "id": "req-123",
  "success": true,
  "backup_file": "backup-2024-08-11.gitdb",
  "file_size": "2.3GB",
  "location": "/var/backups/mydb/",
  "total_branches": 15,
  "total_commits": 8547,
  "timestamp": "2024-08-11T17:45:00Z",
  "duration_ms": 8532
}
```

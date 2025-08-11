# RESTORE DATABASE

## Description

The RESTORE DATABASE command recovers a SproutDB database from a backup file. It replaces the current database with the contents of the specified backup, including all branches, history, and configurations. This command is crucial for disaster recovery, database migration, or reverting to a known good state.

## Where It Applies

- Used as a standalone command outside of data queries
- Requires admin permission to execute
- Specifies a backup file path on the server
- Is destructive - completely replaces the current database
- Should be used with caution in production environments

## What It Does

- Completely replaces the current database with the backup contents
- Restores all branches and their full commit history
- Restores schema evolution history across all branches
- Restores all admin/token configurations from the meta-database
- Requires a valid backup file created by BACKUP DATABASE
- Returns metadata about the restore operation
- Makes the restored database immediately available for use

## Examples

### Basic database restoration

```sql
restore database from "backup-2024-08-11.gitdb"
```

### Restoring from a specific backup

```sql
restore database from "pre-migration-backup.gitdb"
```

### Disaster recovery restoration

```sql
restore database from "daily-backup-2024-08-10.gitdb"
```

### Restoring to a previous state

```sql
restore database from "stable-version-1.5.gitdb"
```

### Sample response

```json
{
  "id": "req-456",
  "success": true,
  "source_file": "backup-2024-08-11.gitdb",
  "file_size": "2.3GB",
  "restored_branches": 15,
  "restored_commits": 8547,
  "timestamp": "2024-08-11T18:30:00Z",
  "duration_ms": 12453,
  "message": "Database successfully restored. All connections have been reset."
}
```

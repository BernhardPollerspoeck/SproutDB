# SpoutDB Storage Format Documentation

## Overview

SpoutDB uses a branch-based storage system with snapshots and delta files for efficient versioning and time-travel capabilities. Each branch maintains its own set of files for optimal isolation and performance.

**File Size Management:**
- **Default Max File Size:** 100MB per file (database-level setting)
- **Table-Level Override:** Can be configured per table (`null` = infinite size)
- **Chunk Creation:** Files split automatically when size limit reached
- **Examples:** Small tables remain single files, large tables split into multiple chunks

## File Types

### 1. Branch Metadata Files (`branch_{branch-name}.meta`)

Branch metadata files contain references to all files belonging to a branch and their commit ranges for fast navigation.

**Format (Human-Readable):**
```yaml
Branch: feature-x
Base Branch: main
Base Commit: c1400
Files:
  snapshot_feature-x_0001.full: "c1400:0,c1450:1024"
  segment_feature-x_0001.delta: "c1401:0,c1420:256,c1430:384,c1450:448"
  segment_feature-x_0002.delta: "c1451:0,c1460:180,c1475:400,c1487:495"
```

**Format (On-Disk):**
```yaml
Branch: feature-x
Base Branch: main
Base Commit: c1400
Files:
  segment_feature-x_0001.delta: [4 bytes: c1401][8 bytes: 0][4 bytes: c1420][8 bytes: 256][4 bytes: c1430][8 bytes: 384][4 bytes: c1450][8 bytes: 448]
  segment_feature-x_0002.delta: [4 bytes: c1451][8 bytes: 0][4 bytes: c1460][8 bytes: 180][4 bytes: c1475][8 bytes: 400][4 bytes: c1487][8 bytes: 495]
```

**Base Commit Explanation:**
The Base Commit (`c1400`) is the commit where this branch was created/branched off from its parent branch (`main`). All commits before the base commit are inherited from the parent branch and don't need to be stored in this branch's files.

**Purpose:**
- Fast lookup of files containing specific commits with exact byte positions
- Binary format enables direct memory access without string parsing
- Commit IDs and offsets are stored as fixed-size integers
- HEAD commit is always the last commit in the newest delta file

**Write Operations:**
1. **New Commit:** Append to .delta file → Append commit entry to meta file binary data
2. **Segment Rollover:** Save snapshot → Create new .delta file → Add new file entry to .meta file

**Note:** Meta file supports binary appending for existing files, structured format enables efficient parsing without string operations.

---

### 2. Snapshot Files (`snapshot_{branch-name}_{sequence}/`)

Snapshot files contain the complete data state at the time of a specific commit. Large tables are split into chunks for efficient parallel loading and selective access.

**Directory Structure:**
```
/branches/feature-x/snapshots/snapshot_c1450/
├── _meta.yaml                           # Snapshot metadata
├── users.chunk001                       # New chunk file
├── orders.chunk001                      # New chunk file
├── orders.chunk003                      # New chunk file  
├── orders.chunk004                      # New chunk file
├── products/                            # Empty (all chunks linked)
└── analytics_events.chunk002            # New chunk file
    # Note: Missing chunk files are linked to previous snapshots
    # orders.chunk002 → linked to snapshot_c1400/orders.chunk002
    # orders.chunk005 → linked to snapshot_c1400/orders.chunk005
    # products.chunk001 → linked to snapshot_c1400/products.chunk001
```

**Snapshot Metadata (_meta.yaml):**
```yaml
Snapshot ID: snapshot_feature-x_0001
Branch: feature-x
Commit: c1450
Created: 2025-08-12T10:00:00Z
Tables:
  users:
    Total_Rows: 1500
    Columns: [name:string, email:string, city:string]
    Chunks: 1
    Files:
      users.chunk001: null  # New chunk, no link
    
  orders:
    Total_Rows: 2500000
    Columns: [user_id:int, total:decimal, date:date, status:string]
    Chunks: 5
    Files:
      orders.chunk001: null                               # New chunk
      orders.chunk002: "snapshot_c1400/orders.chunk002"   # Link to previous
      orders.chunk003: null                               # New chunk
      orders.chunk004: null                               # New chunk
      orders.chunk005: "snapshot_c1400/orders.chunk005"   # Link to previous
    
  products:  # Completely unchanged table
    Total_Rows: 15000
    Columns: [name:string, price:decimal, category:string]
    Chunks: 1
    Files:
      products.chunk001: "snapshot_c1400/products.chunk001"  # Link to previous

  analytics_events:
    Total_Rows: 50000000
    Columns: [user_id:int, event_type:string, timestamp:date, payload:object]
    Chunks: 20
    Files:
      analytics_events.chunk001: "snapshot_c1400/analytics_events.chunk001"  # Link
      analytics_events.chunk002: null                                        # New chunk
      analytics_events.chunk003: null                                        # New chunk
      # ... chunk004-020 all new (null)
```

**Individual Chunk File (orders.chunk003):**
```yaml
Table: orders
Chunk: 3/5
Rows: 847293  # Variable row count based on file size limit
Row_Range: [1694587, 2541880]  # Actual range based on data
Columns: [user_id:int, total:decimal, date:date, status:string]
Data:
  - [1694587, 1725523200123456, 1, 49.99, "2024-01-01 10:00:00", "paid"]  # [row_id, last_modified, data...]
  - [1694588, 1725523200123457, 1, 19.99, "2024-01-01 11:00:00", "paid"]
  # ... 847,291 more rows (until ~100MB file size reached)
```

**Characteristics:**
- **File-Size Based Chunking:** Large tables split when reaching size limit (default: 100MB)
- **Ordered by ID:** Chunks contain sequential row_id ranges for direct access
- **Parallel Loading:** Multiple chunks can be loaded simultaneously
- **Selective Access:** Only required chunks need to be loaded for queries
- **Size-Based Calculation:** Chunk boundaries determined by file size, not row count
- **Directory-based:** Each snapshot is a directory containing chunk files
- **Compressible:** Individual chunk files can be compressed when not active

**Lazy Snapshot Creation:**
- **On-Demand:** Snapshots created only when needed (branch checkout, recovery)
- **Delta-Chain Limits:** Automatic snapshot when chain exceeds 100 commits or 500MB total size
- **Performance Trade-off:** Faster commits vs. longer recovery time for deep chains
- **Memory Efficiency:** Delta-chains stream from disk, minimal RAM usage during normal operations

**Loading Examples:**
```
Query: "get orders where row_id = 3,456,789"
→ Search through chunks based on row_id ranges in metadata
→ Load appropriate chunk file

Query: "get orders where row_id between 1500000 and 2800000"  
→ Load chunks containing this range (determined from metadata)

Query: "get users"
→ Load users.chunk001 (small table, likely single chunk under 100MB)
```

**Compression:**
- **Active files:** Uncompressed for fast read/write access
- **Read-only files:** Compressed with `.lz4` extension
- **Examples:** `orders.chunk001.lz4`, `users.chunk001.lz4`

**Chunk Deduplication:**
- **Link Strategy:** Identical chunks reference previous snapshots instead of duplicating data
- **Delta Compression:** Near-identical chunks (85%+ similarity) use binary patches instead of full copies
- **Storage Savings:** 30-45% reduction for typical SaaS applications with stable reference data
- **Link Format:** `"snapshot_{commit_id}/{table}.chunk{number}"` or `null` for new chunks
- **Patch Format:** `"snapshot_{commit_id}/{table}.chunk{number} + patch_{commit_id}_{chunk}.delta"`
- **Chain Resolution:** Links and patches can form chains across multiple snapshots
- **Purge Safety:** Reference counting prevents deletion of linked/patched chunks

---

### 3. Delta/Segment Files (`segment_{branch-name}_{sequence}.delta`)

Delta files contain atomic commits with queries that can be applied to the parent data state.

**Format:**
```yaml
Segment: segment_feature-x_0001
Branch: feature-x

c1401:
  Parents: [c1400]
  Timestamp: 1725523200000000
  Author: alice
  Query: INSERT INTO users (row_id,name,email) VALUES (3,"Charlie","charlie@example.com")
  Checksum: A1B2C3D4

c1420:
  Parents: [c1401]
  Timestamp: 1725609900000000
  Author: bob
  Query: UPDATE orders SET total=19.99 WHERE row_id=102
  Checksum: 5E6F7890

c1430:   # Merge commit
  Parents: [c1425, c1428]
  Timestamp: 1725696000000000
  Author: alice
  Query: MERGE feature/new-pricing INTO main
  Checksum: 9A8B7C6D

c1450:
  Parents: [c1445]
  Timestamp: 1725955200000000
  Author: bob
  Query: DELETE FROM orders WHERE row_id=103
  Checksum: 1F2E3D4C
```

**Timestamp Format:**
- **Unix Microseconds:** Integer value representing microseconds since Unix epoch (1970-01-01 00:00:00 UTC)
- **Precision:** Microsecond precision enables high-frequency commit ordering
- **Example:** `1725523200000000` = 2025-08-05 09:00:00.000000 UTC
- **Conversion:** `timestamp_micros = (datetime - epoch).TotalMicroseconds`

**Characteristics:**
- Contains commits with atomic queries applicable to parent data state
- Merge commits contain only the merge operation itself, not individual queries from merged branch
- Commit IDs are globally unique, parent IDs form the DAG
- Stored branch-specifically for optimal performance

**Integrity Protection:**
- **Per-Commit Checksums:** Each commit includes CRC32 checksum for corruption detection
- **Append-Friendly:** Checksums written as part of commit data during append operations
- **Error Handling:** Corrupted commits are skipped during recovery with warning logs
- **Validation:** Checksums verified on every commit read for data integrity

**File Size Management:**
- **Max File Size:** 100MB per segment (default), configurable per database
- **Rollover:** New segment created when size limit reached
- **Variable Commits:** Each segment contains variable number of commits based on data size

**Compression:**
- **Active segment:** Uncompressed for fast writes
- **Read-only segments:** Compressed with `.delta.lz4` extension
- **Space savings:** 40-60% reduction in storage size

---

## Key System Principles

| Component | Purpose | Special Features |
|-----------|---------|------------------|
| **Snapshots** | Complete state at commit time | Compressible, deletable |
| **Delta Files** | Atomic queries on parent state | Branch-specific storage |
| **Branch Metadata** | Fast file and commit lookup | Navigation optimization |
| **Row IDs** | Always included in rows | Unique identification |
| **Column Definitions** | Non-ID columns only | Storage optimization |

## File Naming Conventions

- **Branch Metadata:** `branch_{branch-name}.meta`
- **Snapshots:** `snapshot_{branch-name}_{sequence}/` (directory)
- **Snapshot Metadata:** `snapshot_{branch-name}_{sequence}/_meta.yaml`
- **Snapshot Chunks:** `snapshot_{branch-name}_{sequence}/{table}.chunk{number}`
- **Delta Segments:** `segment_{branch-name}_{sequence}.delta`

## Storage Optimization

- **Column Storage:** Only non-ID columns in definitions, row_id and last_modified always implicit
- **Row-Level Timestamps:** Each row includes `last_modified` timestamp for merge conflict resolution
- **File-Size Based Chunking:** Tables split based on file size limits (default: 100MB)
- **Variable Chunk Sizes:** Chunk boundaries adapt to actual data size and row complexity
- **Table-Level Configuration:** Size limits can be overridden per table (`null` = infinite)
- **Compression:** Individual chunk files can be compressed when not actively used
- **Pruning:** Old snapshots can be deleted when newer snapshots exist
- **Branch Isolation:** Each branch maintains separate file sets for performance
- **Parallel Loading:** Multiple chunks and tables can be loaded simultaneously

## Merge Strategy

- **Row-Level Conflict Resolution:** Each row contains `last_modified` timestamp (Unix microseconds)
- **Consistent Merge Logic:** Latest `last_modified` timestamp always wins for all conflict types
- **Update vs Update:** Row with newer `last_modified` overwrites older version
- **Delete vs Update:** If updated row has newer `last_modified` than delete commit timestamp, update wins
- **Developer Control:** "Touch" rows with empty upserts to bump `last_modified` and prevent unwanted deletes
- **Automatic Timestamps:** `last_modified` set automatically on every insert/update operation

## File Size Configuration

### **Database-Level Settings:**
```yaml
# Default Limits
File_Size_Limits:
  Delta_Segments: 100MB      # Default for .delta files
  Snapshot_Chunks: 100MB     # Default for .chunk files
  
# Table-Level Overrides
Table_Overrides:
  users:
    Snapshot_Chunks: 50MB    # Small chunks for frequent access
  analytics_events:
    Snapshot_Chunks: null    # Unlimited chunks for massive tables
    Delta_Segments: 200MB    # Larger delta files for high-volume writes
  orders:
    Snapshot_Chunks: 150MB   # Larger chunks for batch processing
```

### **Rollover Behavior:**
- **Delta Segments:** New segment created when current file reaches `Delta_Segments` limit
- **Snapshot Chunks:** New chunk created when current chunk reaches `Snapshot_Chunks` limit
- **Table Overrides:** Applied per table, overriding database defaults
- **Unlimited (`null`):** Files grow without size restrictions
- **Configuration:** Settings applied at database startup, changes require restart

## Row ID System

### **Format:**
```
Row ID = timestamp_base62 + branch_id_base62
```

### **Base62 Encoding:**
- **Alphabet:** `0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz`
- **Properties:** URL-safe, human-readable, preserves sort order
- **Length:** ~12-13 characters per ID

### **Branch ID Management:**
```yaml
# Sequential branch counter (collision-free)
Branch_IDs:
  main: 1
  feature/pricing: 2
  feature/ProduktX/ABSEDN-12345: 3
  customer-a/prod: 4
  # ... up to 65,535 branches
```

### **ID Examples:**
```
main (branch_id=1):         "dQw4w9WgXcQ1"     # timestamp + "1"
feature/pricing (id=2):     "dQw4w9WgXcR2"     # timestamp + "2"  
feature/ProduktX (id=3):    "dQw4w9WgXcS3"     # timestamp + "3"
```

### **Collision Avoidance:**
- **Sequential Branch IDs:** Guaranteed collision-free across branches
- **Microsecond Timestamps:** Single writer per table prevents same-microsecond conflicts
- **Increment Logic:** If timestamp collision detected, increment microsecond component
- **Overflow Handling:** After 65k branches, reuse deleted branch IDs or roll over

### **Properties:**
- **Sortable:** Timestamp-based ordering preserved in Base62
- **Compact:** 40% smaller than decimal representation
- **Human-Readable:** Copy-pasteable, debugger-friendly
- **Collision-Free:** Mathematical guarantee via sequential branch allocation

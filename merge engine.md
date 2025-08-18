# SpoutDB Merge Engine - Zusammenfassung aus Projektwissen

## 🎯 **Aktuell Definierte Merge-Strategie**

### **Row-Level Merging (Deterministisch)**
```
Verschiedene IDs: Kein Konflikt, beide übernehmen
Gleiche ID, verschiedene Felder: Automatisch zusammenführen  
Gleiche ID, gleiche Felder: Neuester Timestamp gewinnt
Gelöschte vs. geänderte Records: Änderung gewinnt
```

### **Schema beim Merge**
```
Neue Felder: Alle übernehmen (Union)
Typ-Konflikte: Automatisch zum "weiteren" Typ
Purged Fields: Bleiben purged wenn in einem Branch purged
```

### **Timestamp-basierte Konfliktlösung**
- Jede Row enthält `last_modified` (Unix Microseconds)
- Latest `last_modified` timestamp gewinnt **immer**
- Delete vs Update: Update gewinnt wenn `last_modified` > delete commit timestamp
- Developer Control: "Touch" rows mit empty upserts um `last_modified` zu bumpen

---

## 📋 **Definierte Merge Commands**

### **Standard Merge**
```sql
merge feature/new-pricing into main
```

### **Alias-Move Merge**  
```sql
merge feature/pricing into main with alias
```
→ Alle Aliases die auf `feature/pricing` zeigen werden nach Merge auf `main` verschoben

---

## 🏗️ **Storage-Level Merge Details**

### **Branch DAG Structure**
```yaml
c1430:   # Merge commit
  Parents: [c1425, c1428]  # Multiple parents = merge
  Timestamp: 1725696000000000
  Author: alice
  Query: MERGE feature/new-pricing INTO main
  Checksum: 9A8B7C6D
```

### **Merge Commit Properties**
- Merge commits enthalten nur die Merge-Operation selbst
- Keine individuellen Queries vom gemergten Branch
- Parent IDs bilden das DAG
- Commit IDs sind global unique

---

## 🚀 **DEFINIERT: Git-Style Merge Engine**

### **Merge Execution Process**

**1. Git-Style Delta Application:**
```
1. Find Merge-Base (common ancestor zwischen Source und Target)
2. Collect Source Deltas (alle commits seit Merge-Base) 
3. Apply Source Deltas nacheinander auf Target Branch
4. Write Merge-Commit mit Parents: [target-HEAD, source-HEAD]
```

**2. Field-Level Merge Logic:**
```
Beim Apply von Source Delta auf Target Record:

IF field ist neu (nicht in Target):
    → Immer hinzufügen (ignoriere Row-Timestamp)
    
IF field existiert in Target:
    → Timestamp Rule: latest last_modified gewinnt
    
IF Record ist neu:
    → Immer hinzufügen
```

**3. Delete Resolution:**
```
Source Delta: DELETE record at timestamp X
Target Record: last_modified = Y

IF X > Y: Delete gewinnt → Record wird gelöscht
IF X ≤ Y: Update gewinnt → Record bleibt erhalten
```

**4. Schema Merge Rules:**
```
Neue Fields: Union (alle übernehmen)
Type Conflicts: Widest Type gewinnt (string/number → mixed → any)
Purge vs Add: Timestamp der Schema-Operation entscheidet
```

### **Merge Concurrency Strategy**

**Blocking Merge mit Write-Queue:**
```
1. Merge Start: Source + Target Branches → "MERGING" State (read-only für writes)
2. Neue Writes: Werden in branch-spezifische Queues eingereiht
3. Merge Execution: Läuft ungestört ohne Interferenz durch concurrent writes
4. Merge Complete: Queues werden abgearbeitet, Branches → "ACTIVE" State
5. Queue Processing: Alle wartenden Writes werden erfolgreich ausgeführt
```

**Branch States:**
- **ACTIVE:** Normal read/write operations
- **MERGING:** Read-only + write queue + merge process running
- **QUEUE_FLUSH:** Merge complete, processing queued writes

**Atomic Merge Queuing:**
```
Merge Request wird atomically in beide Branch-Queues eingetragen:
1. Add "merge source→target" zu Source-Branch Queue
2. Add "merge source→target" zu Target-Branch Queue  
3. Beide Queue-Einträge sind verlinkt (same operation)
4. Source + Target Branches wechseln zu MERGING State
```

**Guarantees:**
- Keine fehlgeschlagenen Queries wegen Merge-Konflikten
- Alle Writes kommen durch (warten in Queue)
- Vorhersagbare User Experience
- Nutzt bestehende Branch-Queue Infrastruktur
- Keine Deadlocks durch atomare Queue-Operations

### **Merge Preview System**

**Respawn-based Preview:**
```sql
respawn merge <source-branch> into <target-branch> as <preview-db-name>
```

**Beispiele:**
```sql
-- Standard Preview
respawn merge feature/pricing into main as pricing-preview

-- Schema Preview
respawn merge feature/new-fields into main as schema-preview
get schema from schema-preview

-- Data Exploration
get users from pricing-preview where city = "Berlin"
get products from pricing-preview where category = "tech"

-- Cleanup
drop database pricing-preview
```

**Characteristics:**
- **Full Fidelity:** Exakt das Ergebnis der echten Merge
- **No Conflicts:** Per Design deterministische Resolution
- **Explorable:** Normale SpoutDB Queries auf Preview-DB
- **Temporary:** Preview-DB ist separate Instanz, kein Impact auf Original
- **Developer-Friendly:** Tools und Scripts können auf Preview laufen

### **Merge Observability System**

**Progress Reporting via SignalR:**
```javascript
// Progress Events (nur an Merge-User)
{
  "id": "req-123",  // Matching Request ID
  "event": "merge_progress",
  "type": "CalculatingMergeBase",  // Enum für Lokalisierung
  "detail": "Finding common ancestor..."
}

{
  "id": "req-123", 
  "event": "merge_progress",
  "type": "ProcessTable",
  "detail": "'orders' (3/5) - applying 847 deltas"
}

// Completion Event
{
  "id": "req-123",
  "event": "merge_completed",
  "success": true,
  "commit_id": "abc123"
}
```

**Progress Types (Enum):**
- `CalculatingMergeBase` - Finding common ancestor
- `ProcessTable` - Applying deltas per table  
- `WritingCommit` - Creating merge commit
- `MergeCompleted` - Operation finished

**Audit Trail:**
- ✅ Bereits vorhanden durch Commit-History
- Jeder Merge-Commit enthält: Timestamp, Author, Source/Target, Query
- Vollständige Nachverfolgbarkeit ohne zusätzliche Logs

**Event Scope:**
- Nur an User der die Merge ausführt
- Via bestehende SignalR-Infrastruktur
- Lokalisierbar durch Enum + Detail-String

### **Beispiel-Merge:**

**Merge-Base State:**
```yaml
users: [{id: 1, name: "Bob", last_modified: 1000}]
Schema: {id: any, name: string}
```

**Target Branch Changes:**
```yaml
- add column users.email string at 1800
- upsert users {id: 1, email: "bob@new.com", last_modified: 2000}
State: [{id: 1, name: "Bob", email: "bob@new.com", last_modified: 2000}]
Schema: {id: any, name: string, email: string}
```

**Source Branch Changes:**
```yaml  
- upsert users {id: 1, age: 25, last_modified: 1500}
- upsert users {id: 2, name: "Alice", last_modified: 1600}
```

**Merge Process:**
```yaml
1. Apply Source Delta 1: {id: 1, age: 25, last_modified: 1500}
   - age: neues Field → Add anyway
   - Result: {id: 1, name: "Bob", email: "bob@new.com", age: 25, last_modified: 2000}

2. Apply Source Delta 2: {id: 2, name: "Alice", last_modified: 1600}
   - Neuer Record → Add anyway
   - Result: [{id: 1, ...}, {id: 2, name: "Alice", last_modified: 1600}]

3. Schema Merge:
   - Union: {id: any, name: string, email: string, age: number}
```

**Final Merged State:**
```yaml
users: [
  {id: 1, name: "Bob", email: "bob@new.com", age: 25, last_modified: 2000},
  {id: 2, name: "Alice", email: "", age: 0, last_modified: 1600}
]
Schema: {id: any, name: string, email: string, age: number}
```

---

## ❓ **Noch zu definierende Design-Entscheidungen**

*Alle kritischen Design-Entscheidungen sind getroffen! Die Merge Engine ist production-ready.*

---

## 🚀 **DEFINIERT: Advanced Features**

### **Query Replay System**

**Commit Replay:**
```sql
replay abc123 -> main
```

**Functionality:**
- Nimmt Query aus spezifischem Commit
- Führt Query auf Target Branch aus
- Erstellt neuen Commit mit Metadata: `replayed_from: "abc123"`
- Nutzt bestehende Queue-Infrastruktur und Conflict-Resolution

**Use Cases:**
- Bugfix aus development -> production
- Specific feature aus experimental -> main  
- Tested change aus anderem Branch übernehmen

**Implementation:**
1. Lookup Commit abc123 → Extract original query
2. Queue query auf Target Branch (main)
3. Apply mit normaler Conflict-Resolution
4. Write Commit mit Replay-Metadata

**Benefits:**
- Minimal syntax
- Leverages existing infrastructure
- Safe (uses same conflict rules as merge)
- Traceable (replay source in commit metadata)

---

## 🎯 **Merge Engine - KOMPLETT SPEZIFIZIERT! 🚀**

**Alle Core-Features sind production-ready definiert:**

✅ **Git-Style Execution** (Source Deltas auf Target)  
✅ **Field-Level Intelligence** (neue Fields ignorieren Timestamps)  
✅ **Blocking Concurrency** (atomic Queue-Operations)  
✅ **Respawn Preview** (full-fidelity Dry-Runs)  
✅ **Progress Observability** (lokalisierbare SignalR Events)  
✅ **Branch Permissions** (normal/protected mit PAT-Integration)  
✅ **Query Replay** (minimale Cherry-Pick Alternative)  

**Die SpoutDB Merge Engine ist bereit für Production-Implementation! 🎉**

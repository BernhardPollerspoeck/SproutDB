# Datenbank Features - Notizen

---

# Query Language Syntax

## Grundstruktur
```
get [table/alias] 
  [follow joins]
  [where conditions]
  [group by fields]
  [having conditions]
  [order by fields]
  [select fields]
```

## Einfache Queries
```
get users
get users where age > 25
get products order by price desc
get orders where date last 7 days
```

## Projektion
```
get users.name, users.email where city = "Berlin"
get products.title, products.price where category in ["tech", "books"]
```

## Joins (Pfad-Syntax mit Pflicht-Alias)
```
get users 
  follow users.id -> orders.user_id as orders
  follow orders.id -> order_items.order_id as items
  follow items.product_id -> products.id as products
  where orders.total > 500
  select users.name, products.title, items.quantity
```

## Join-Typen
```
follow users.id -> orders.user_id as orders (left)
follow users.id -> orders.user_id as orders (inner)  
follow users.id -> orders.user_id as orders (right)
```

## Aggregationen
```
count users where active = true
sum orders.total where date this month
avg products.price group by category
```

## Gruppierung und Having
```
get users 
  where orders.total > 1000 
  group by city 
  having count > 10
  order by avg(age) desc
```

## Nested/Hierarchische Daten
```
get users where address.country = "Germany" and skills contains "Python"
get posts where comments.any(author = "john" and upvotes > 5)
```

## Zeitbasierte Filter
```
where date last 7 days
where date this month
where date > "2024-01-01"
```

## Data Manipulation (nur 3 Kommandos)

### Tabellen-Struktur
- Jede Tabelle hat `id` (any, pflicht, DB-generiert als wachsende base 128)
- Alle anderen Felder sind optional

### Upsert (insert or update)
```
// Minimal (nur ID wird generiert)
upsert users {}

// Normal 
upsert users {name: "John", age: 25}

// Mit expliziter Suchspalte (Feld MUSS im JSON enthalten sein)
upsert users {email: "john@test.com", name: "John"} on email
```

**Upsert Logik:**
1. Suche Record mit dem Wert der `on` Spalte
2. Gefunden? → Update mit neuen Daten
3. Nicht gefunden? → Erstelle neuen Record

**Ohne `on` clause:**
```
upsert users {name: "John"}           // Default: on id (neuer Record)
upsert users {id: 123, name: "John"}  // Update Record 123 oder erstelle mit ID 123
```

### Bulk Upsert
```
upsert users [
  {name: "John", age: 25},
  {name: "Jane", age: 30},
  {name: "Bob", age: 35}
] on email
```

### Delete
```
delete users where age < 18
delete users where last_login before 1 year
```

### Get (bereits oben definiert)
```
get users where active = true
```

**Copy-Paste Workflow:**
Query Results kommen im selben JSON-Format wie Upsert erwartet:
```
// Query Result:
[{id: 1, name: "John", age: 25}]

// Direct Copy-Paste:
upsert backup_users [{id: 1, name: "John", age: 25}] on id
```

## Schema Evolution (automatisch & vorhersagbar)

### Table Creation & Deletion

```sql
// Create a new empty table (only id column is automatically added)
create table users

// Drop an existing table and all its data
drop table old_users

// Drop table for critical data (use with caution)
drop table customers
```

**Permissions Required:**

- Creating tables requires `schema` permission
- Dropping tables requires `schema` permission
- Both operations create a schema change record in branch history

### Automatische Typ-Erweiterung
```
age: 25           -> number
age: "twenty"     -> mixed (string | number)  
age: true         -> any
created: "2024-08-11 14:30:25.1234" -> date
```

### Typ-Hierarchie (immer erweitern, nie brechen)
```
string -> mixed (string | number) -> any
number -> mixed (string | number) -> any
date -> any
boolean -> any
array -> any
object -> any
```

### Data Types
**Date Format:** UTC only, `yyyy-MM-dd HH:mm:ss.ffff`
```
upsert users {name: "John", created: "2024-08-11 14:30:25.1234"}
get users where created > "2024-01-01 00:00:00.0000"
get users where created last 7 days
```

### Neue Felder
```
// Schema: {id: any, name?: string, age?: number}
upsert users {name: "John", age: 25, city: "Berlin"}
// Schema: {id: any, name?: string, age?: number, city?: string}
```

### Schema Commands
```
add column users.premium boolean    // Erstelle oder restore Feld
purge column users.old_field       // Ausblenden (reversibel)
```

**Add Logic:**
1. Feld nicht vorhanden? → Erstelle mit Typ
2. Feld gepurged? → Restore, Typ bleibt der "weitere"
3. Feld aktiv? → Typ wird erweitert falls nötig

## Git-Style Versioning & Time Travel

### Commits & Branches
```
get users on branch feature/pricing
get users as of commit abc123
get users as of "2024-01-15"
get users as of 3 months ago
```

### Branching
```
create branch feature/new-pricing from main
checkout branch feature/new-pricing
merge feature/new-pricing into main
```

### Merge-Strategie (immer gleich, deterministisch)
**Row-Level Merging:**
- Verschiedene IDs: Kein Konflikt, beide übernehmen
- Gleiche ID, verschiedene Felder: Automatisch zusammenführen
- Gleiche ID, gleiche Felder: **Neuester Timestamp gewinnt**
- Gelöschte vs. geänderte Records: Änderung gewinnt

**Schema beim Merge:**
- Neue Felder: Alle übernehmen (Union)
- Typ-Konflikte: Automatisch zum "weiteren" Typ
- Purged Fields: Bleiben purged wenn in einem Branch purged

**Prinzip:** Merge geht immer, verliert nie Daten, ist vorhersagbar

### Respawn (History-Clean Export)
```
respawn branch <branch-name> as <new-db-name> [since <date>]
```

**Beispiele:**
```
respawn branch customer-a/prod as customer-a-export
respawn branch main as backup-2024 since "2024-01-01"
respawn branch feature/cleanup as fresh-start
```

**Was passiert:**
1. Nimmt aktuellen Stand vom Branch (optional ab Cutoff-Date)
2. Generiert komplett neue, separate DB-Instanz
3. Neue DB startet mit "Initial Commit" (keine History)
4. Original bleibt unverändert

**Use Cases:**
- Whitelabel Export für Kunden
- DSGVO-konformer History-Clean
- Cleanup nach Jahren der Entwicklung
- Migration ohne History-Ballast
- Compliance-konforme Daten-Purge

### Branch Management
```
create branch feature/new-pricing from main
checkout branch feature/new-pricing
merge feature/new-pricing into main
delete branch feature/old-stuff                    // Nur ungemergte Branches
abandon branch customer-backup/2024 with message "Legal backup - DO NOT TOUCH"
reactivate branch customer-backup/2024             // Admin-only

// Branch Protection (Admin-only)
protect branch main                                 // Macht Branch protected
unprotect branch old-main                          // Macht Branch normal
list branches protected
```

**Abandoned Branches:**
- Read-only, keine neuen Commits
- Kann nicht gemerged oder gelöscht werden
- Bleibt für immer erhalten
- Für Legal/Compliance/Audit-Zwecke

**Protected vs Normal Branches:**
- `normal`: Standard Branches (feature/*, bugfix/*)
- `protected`: Kritische Branches (main, prod, customer-*/*)
- Permissions können getrennt vergeben werden

## Auth & Permissions

### Auth Methods
```
auth: none     // Für lokale/embedded Nutzung
auth: pat      // Personal Access Tokens
```

### Connection Strings
```
mydb://                               // No auth (local)
mydb://pat:abc123def@server/         // PAT auth
```

### Token Management (Admin-only)
```
// Erstellen
create token "dev-team" with {
  operations: [read, write, schema, branch],
  tables: [users, products],
  branches: [normal]
}

// Bearbeiten
update token "dev-team" set operations: [read, write, schema, branch, merge]
update token "dev-team" add tables: [orders]
update token "dev-team" remove operations: [schema]

// Abrufen
get token "dev-team"
list tokens
list tokens with operations: [admin]

// Löschen
revoke token "dev-team"
```

### Permission-Typen
**Operations:** `read`, `write`, `schema`, `branch`, `merge`, `admin`, `tokens`

**Tables:** 
- `tables: [users, orders]` - spezifische Tables
- `tables: [*]` - alle Tables

**Branches:**
- `normal` - Standard Branches (feature/*, bugfix/*)
- `protected` - Kritische Branches (main, prod, customer-*/*)

**Meta-DB:** Tokens und Permissions werden in eigener System-DB gespeichert

### Admin-Hierarchie

**Admin-Typen:**
- `Root Admin`: Kann alles, wird bei DB-Setup erstellt, unveränderlich
- `Child Admins`: Können weitere Childs erstellen (mit `create_admin` permission)

**Admin Management:**
```
// Admin erstellen (benötigt create_admin permission)
create admin "team-lead" with {
  operations: [admin, tokens, create_admin],
  tables: [*],
  branches: [normal, protected]
}

// Admin deaktivieren/aktivieren (nur eigene Childs oder Baum runter)
disable admin "team-lead"
enable admin "team-lead"

// Admin löschen (nur Root)
delete admin "team-lead"

// Baum anzeigen
list admins tree
list admins under "team-lead"
```

**Deaktivierungs-Kaskade:**
```
disable admin "team-lead"
```
**Automatisch:**
- `team-lead` + alle Child-Admins werden deaktiviert
- ALLE Tokens (von team-lead + Childs) werden deaktiviert

**Reaktivierung (manuell):**
```
enable admin "team-lead"              // Nur team-lead aktiviert
enable token "dev-token-123"          // Tokens einzeln aktivieren
enable admin "feature-lead"           // Childs einzeln aktivieren
```

**Granulare Token-Kontrolle:**
```
disable token "dev-token-123"                // Token in eigenem Baum deaktivieren
disable token under admin "feature-lead"    // Alle Tokens eines Childs
enable token "dev-token-123"                // Einzeln reaktivieren
list tokens by admin "team-lead"            // Nur direkte Tokens
list tokens under admin "team-lead"         // Inklusive Child-Baum
```

**Regeln:**
- Tokens können nie mehr Permissions haben als der erstellende Admin
- Jeder Admin kann nur eigene Childs und deren Baum verwalten
- Child kann nie Parent verwalten

## Remote Connections & Real-time

### Connection Type: SignalR Only
```
// SignalR Connection
signalr://pat:abc123@server:8080/mydb
```

### Real-time Branch Subscriptions
```
// Branch-Subscriptions
subscribe to branch main
subscribe to branch feature/pricing where table = "products"
subscribe to branch main where table in ["users", "orders"] and operation = "upsert"

// Unsubscribe
unsubscribe from branch main
unsubscribe all
```

**Events:**
```
{
  "branch": "main",
  "table": "users", 
  "operation": "upsert",
  "record": {id: 123, name: "John", age: 26},
  "timestamp": "2024-08-11T14:30:00Z",
  "commit": "abc123"
}
```

**Use Cases:**
- Live Dashboards
- Real-time UI Updates  
- Service-to-Service Sync
- Feature-Team Notifications

### Unified Message Format

**Request:**
```
{
  "id": "req-123",
  "query": "get users where age > 25",
  "branch": "main"
}
```

**Response (mit matching ID):**
```
{
  "id": "req-123",
  "branch": "main",
  "table": "users",
  "operation": "get", 
  "data": [...],
  "success": true,
  "timestamp": "2024-08-11T14:30:00Z",
  "commit": "abc123"
}
```

**Event (keine ID):**
```
{
  "branch": "main",
  "table": "users",
  "operation": "upsert",
  "data": [{id: 123, name: "John"}],
  "success": true,
  "timestamp": "2024-08-11T14:30:00Z", 
  "commit": "def456"
}
```

**Vorteil:** Response und Event haben identische Struktur

### History & Audit

**History Queries:**
```
get history for branch main
get history for branch main since "2024-01-01" 
get history for table users on branch main
get history between commit abc123 and def456

get commit abc123
get changes in commit abc123
get diff between commit abc123 and def456

get merge-base branch feature/pricing with main
get conflicts between branch feature/pricing and main
```

## Performance & Indexing

### Manual Indexing
```
create index users_email on users (email)
create index users_age_city on users (age, city) 
drop index users_old_index
```

### Intelligent Query Planner
```
explain get users where email = "john@test.com" and age > 25
```

**Planner Output:**
```
Query Plan:
1. Table Scan: users (SLOW - 50ms, 10k rows scanned)
   → Suggestion: create index users_email_age on users (email, age)
   
2. Filter: age > 25 (2ms)
3. Total: 52ms

Recommendations:
- Index missing for email column (would reduce scan to 1ms)
- Consider composite index for email+age filters
- Unused indexes: users_old_name_index
```

**Features:**
- Zeigt Performance-Hotspots
- Schlägt konkrete Indexes vor
- Geschätzte Verbesserungen
- Erkennt ungenutzte Indexes

## Scaling: Read Replicas

### Connection Discovery (SignalR)
```
// Client fragt beim Connect nach verfügbaren Replicas
{
  "id": "req-123",
  "query": "get replicas"
}
```

### Unified Replica Messages
**Response (mit ID):**
```
{
  "id": "req-123",
  "replicas": [
    {"url": "signalr://replica1.db:8080", "lag": "~50ms", "load": "23%", "status": "online"},
    {"url": "signalr://replica2.db:8080", "lag": "~180ms", "load": "67%", "status": "online"}
  ],
  "success": true
}
```

**Status Update (keine ID):**
```
{
  "replicas": [
    {"url": "signalr://replica2.db:8080", "lag": "~180ms", "load": "67%", "status": "offline", "reason": "maintenance"},
    {"url": "signalr://replica3.db:8080", "lag": "~45ms", "load": "12%", "status": "online"}
  ],
  "success": true
}
```

**Features:**
- Client erhält nur online Replicas beim Connect
- Live Updates wenn Replicas offline/online gehen
- Automatisches Failover auf andere Replicas
- Multiple SignalR Connections: Main + beste Replicas
- Automatic Routing: Write → Main, Read → beste verfügbare Replica

## Limits & Performance

### Query Timeout
```
// Server Config: server_max_timeout: 30000ms

// Client Request (timeout darf server_max nicht überschreiten)
{
  "id": "req-123",
  "query": "get users where age > 25",
  "timeout": 5000
}

// Timeout Response
{
  "id": "req-123", 
  "success": false,
  "error": "QUERY_TIMEOUT",
  "timeout_ms": 5000
}
```

### Rate Limiting (Optional)
```
// Token Config: rate_limit: {enabled: true, requests_per_minute: 1000}

// Rate Limit Response
{
  "id": "req-456",
  "success": false, 
  "error": "RATE_LIMITED",
  "retry_after": "2024-08-11 14:31:25.0000",
  "requests_remaining": 0
}
```

**Prinzipien:**
- **Keine DB Size Limits** - flexibel skalierbar
- **Client Timeout ≤ Server Max** - vorhersagbar
- **Rate Limiting optional** - für spezielle Use Cases (Reporting Tools)
- **Clear Error Messages** - retry Information verfügbar

## Backup & Recovery

### Full Database Backup (benötigt `admin` permission)
```
backup database to "backup-2024-08-11.gitdb"
```

**Was wird gesichert:**
- Komplette Datenbank mit ALLEN Branches
- Alle Commits (komplette History)
- Schema-Evolution-History  
- Admin/Token-Konfiguration (Meta-DB)

### Restore (benötigt `admin` permission)
```
restore database from "backup-2024-08-11.gitdb"
```

**Response:**
```
{
  "id": "req-123",
  "success": true,
  "backup_file": "backup-2024-08-11.gitdb",
  "file_size": "2.3GB",
  "location": "/var/backups/mydb/",
  "total_branches": 15,
  "total_commits": 8547
}
```

**Prinzipien:**
- **Ein Command = Komplette DB** - simpel und vollständig
- **Lokale Ablage** - Files auf Server, Admin holt manuell ab
- **Restore ist destructive** - ersetzt komplette Datenbank
- **Alles oder nichts** - keine Teil-Backups/Restores

### Server-Level Backup (benötigt `admin` permission)
```
backup server to "server-backup-2024-08-11.gitdb"
```

**Was wird gesichert:**
- Server-Meta-DB (globale PATs, Users, Settings)
- Server-Konfiguration
- **KEINE Datenbanken** (die sind standalone)

**Separate Backups:**
- Server: Server-Config + globale Auth
- Database: Komplette einzelne Datenbank

## Error Handling

### Standard Error Responses
```javascript
// Query-Syntax-Fehler
{
  "id": "req-123",
  "success": false,
  "error": "SYNTAX_ERROR",
  "message": "Invalid query: missing 'where' clause",
  "position": 15
}

// Schema-Verletzung
{
  "id": "req-456", 
  "success": false,
  "error": "SCHEMA_ERROR",
  "message": "Field 'email' already exists with different type"
}
```

## Configuration Management

### PAT Storage
- **DB-spezifische PATs:** Gehören in die jeweilige DB
- **Globale/Server-PATs:** Gehören in separate Server-Meta-DB
- **Server Settings:** Eigene Server-DB (Memory, Disk, etc.)

## Disaster Recovery - Replica Promotion

### Primary Down Detection
```javascript
{
  "event": "primary_down_detected",
  "primary": "main.db:8080",
  "detected_by": ["replica1", "replica2", "replica3"]
}
```

### Sync Verification & Election
```javascript
// Erst wenn alle auf gleichem Stand sind
{
  "event": "sync_verification",
  "replicas": [
    {"url": "replica1", "last_commit": "abc123", "synced": true},
    {"url": "replica2", "last_commit": "abc123", "synced": true},
    {"url": "replica3", "last_commit": "abc122", "synced": false}
  ],
  "waiting_for_sync": ["replica3"]
}

// Dann Voting
{
  "event": "election_started",
  "candidates": [
    {"url": "replica1", "lag": "10ms", "load": "15%"},
    {"url": "replica2", "lag": "45ms", "load": "67%"}
  ]
}
```

### Client Promotion (≥50% Quorum)
```javascript
// Alle Replicas senden Entscheidung an Client
{
  "event": "new_primary_elected",
  "new_primary": "replica1.db:8080",
  "votes": "3/4",
  "quorum_reached": true
}
```

**Prozess:**
1. Alle Replicas merken Primary-Ausfall
2. Sync-Check: Voting erst wenn alle auf gleichem Stand
3. Replicas wählen neuen Primary basierend auf Load & Lag
4. Konsens wird an Client gesendet
5. Client akzeptiert erst bei ≥50% Quorum
6. Neuer Primary muss bereit für Connections sein

## Replica Sync Strategy

### Write Replication: Hub-and-Spoke
```javascript
// Primary sendet direkt an alle Replicas
Primary → [Replica1, Replica2, Replica3, ...]

// Write Response nach erfolgreicher Replikation
{
  "id": "req-123",
  "success": true,
  "replicated_to": ["replica1", "replica2", "replica3"],
  "replication_lag": "~50ms"
}
```

**Vorteile:**
- Vorhersagbare Reihenfolge
- Einfach zu debuggen
- Garantierte Konsistenz
- Bei Bottleneck: Load-Balancer vor mehrere Primary Server

### Membership & Health: Gossip Protocol
```javascript
// Für nicht-kritische Informationen
- Node Health Status
- Membership Changes  
- Configuration Updates
- Replica Discovery
```

**Prinzipien:**
- **Write-Sync:** Hub-and-Spoke für Konsistenz
- **Membership:** Gossip für Skalierbarkeit
- **Retry/Queue:** Bei Replica-Ausfällen
- **Async Replication:** Mit Lag-Monitoring

## Data Types & Validation

**Kein NULL-Werte:**
- Numbers: Default `0`
- Strings: Default `""`
- Arrays: Default `[]`
- Objects: Default `{}` (JSON storage)

**Object/JSON Storage:**
```javascript
upsert users {
  name: "John", 
  profile: {
    settings: {theme: "dark", notifications: true},
    preferences: ["email", "sms"]
  }
}

get users where profile.settings.theme = "dark"
```

---

# Database Design Komplett

Alle Features sind definiert und dokumentiert!
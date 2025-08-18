# SpoutDB Query Macros & Conditional Writes

## 🎯 **Feature Overview**

Zwei neue Features für SpoutDB, die auf der bestehenden Architektur aufbauen:

1. **Query Macros** - Wiederverwendbare Query-Sequenzen mit Parametern
2. **Conditional Writes** - Optimistic Locking für sichere Multi-User-Updates

---

## 🔧 **Query Macros**

### **Konzept**
Macros sind benannte, parametrisierte Query-Sequenzen die als atomare Einheit ausgeführt werden.

### **Syntax**

**Macro Definition:**
```sql
create macro <macro-name>(<parameters>) as [
  "query 1 with {parameter} substitution",
  "query 2 with {parameter} substitution", 
  "query N..."
]
```

**Macro Ausführung:**
```sql
@<macro-name>(<arguments>)
```

**Macro Management:**
```sql
get macros
get macro <macro-name>
drop macro <macro-name>
```

### **Beispiele**

**Customer Setup Macro:**
```sql
create macro customer_setup(name) as [
  "add column customers.{name}_notes text",
  "add column orders.{name}_priority number",
  "create token '{name}-readonly' with operations: [read], tables: [customers, orders]"
]

-- Verwendung
@customer_setup("acme")
```

**Testing Data Macro:**
```sql
create macro create_test_data(user_count, order_count) as [
  "upsert users [{name: 'Test User 1', email: 'test1@example.com'}, {name: 'Test User 2', email: 'test2@example.com'}]",
  "upsert orders [{user_id: 1, total: {order_count}, status: 'paid'}]"
]

@create_test_data(10, 250)
```

**Database Cleanup Macro:**
```sql
create macro cleanup_old_data(days) as [
  "delete orders where date before {days} days and status = 'cancelled'",
  "delete users where last_login before {days} days and email = ''",
  "purge column users.temp_field"
]

@cleanup_old_data(90)
```

### **Parameter Substitution**

**String Interpolation:**
```sql
create macro add_customer_fields(customer) as [
  "add column users.{customer}_tier string",
  "add column users.{customer}_discount number"
]

@add_customer_fields("premium")
-- Wird zu:
-- add column users.premium_tier string
-- add column users.premium_discount number
```

**Numeric Parameters:**
```sql
create macro batch_update_pricing(multiplier, category) as [
  "upsert products all {price: price * {multiplier}} where category = '{category}'"
]

@batch_update_pricing(1.1, "Shoes")  -- 10% Preiserhöhung für Shoes
```

### **Response Format**

```json
{
  "id": "req-123",
  "success": true,
  "macro_name": "customer_setup",
  "parameters": {"name": "acme"},
  "executed_queries": 3,
  "results": [
    {"query": "add column customers.acme_notes text", "success": true},
    {"query": "add column orders.acme_priority number", "success": true},
    {"query": "create token 'acme-readonly'...", "success": true, "token": "abc123"}
  ],
  "timestamp": "2024-08-11T14:30:00Z"
}
```

### **Error Handling**

```json
{
  "id": "req-456",
  "success": false,
  "macro_name": "customer_setup",
  "parameters": {"name": "invalid-name"},
  "error": "MACRO_EXECUTION_FAILED",
  "failed_at_query": 2,
  "failed_query": "create token 'invalid-name-readonly'...",
  "error_details": "Token name contains invalid characters",
  "executed_queries": 1,
  "rollback_performed": true
}
```

### **Macro Storage**

**Branch-Specific:**
- Macros werden pro Branch gespeichert (wie Schema-Commits)
- Macros können branch-spezifische Anpassungen haben
- Macros werden mit Branch gemerged

**Storage Format:**
```yaml
# _meta table commit
Operation: create_macro
Macro_Name: customer_setup
Parameters: [name]
Queries: [
  "add column customers.{name}_notes text",
  "add column orders.{name}_priority number",
  "create token '{name}-readonly' with operations: [read], tables: [customers, orders]"
]
Author: alice
Timestamp: 2024-08-11T14:30:00Z
```

### **Permissions**

**Macro Creation:**
- Erfordert `schema` permission
- Alle Queries im Macro müssen mit User-Permissions ausführbar sein

**Macro Execution:**
- User muss alle Permissions haben, die die enthaltenen Queries erfordern
- Bei fehlenden Permissions: Klare Fehlermeldung welche Permission fehlt

### **Use Cases**

1. **Customer Onboarding:** Standardisierte Schema-Setups pro Kunde
2. **Development Workflows:** Wiederholbare Test-Daten-Generierung
3. **Maintenance Tasks:** Regelmäßige Cleanup-Operationen
4. **Schema Migration:** Komplexe Schema-Änderungen als atomare Unit
5. **Token Management:** Standardisierte Permission-Setups für Teams

---

## 🔒 **Conditional Writes (Optimistic Locking)**

### **Konzept**
Conditional Writes ermöglichen es, Updates nur dann auszuführen wenn bestimmte Bedingungen erfüllt sind - primär für Optimistic Locking.

### **Syntax**

```sql
upsert <table> <data> if <condition>
delete <table> where <filter> if <condition>
```

### **Beispiele**

**Basis Conditional Update:**
```sql
-- Update nur wenn Record unverändert ist
upsert users {id: 123, name: "John Smith", age: 26} 
  if last_modified = "2024-08-11 14:30:00.1234"
```

**Conditional Delete:**
```sql
-- Delete nur wenn Status unverändert ist
delete orders where id = 456 
  if status = "pending"
```

**Multiple Conditions:**
```sql
-- Update nur wenn mehrere Felder unverändert
upsert orders {id: 789, total: 99.99, status: "paid"} 
  if last_modified = "2024-08-11 14:30:00.1234" and status = "pending"
```

**Version-Based Locking:**
```sql
-- Mit explizitem Version-Counter
upsert products {id: 111, price: 49.99, version: 5} 
  if version = 4
```

### **Response Formats**

**Successful Conditional Write:**
```json
{
  "id": "req-123",
  "success": true,
  "operation": "upsert",
  "table": "users",
  "condition_met": true,
  "affected_rows": 1,
  "data": {
    "id": 123,
    "name": "John Smith", 
    "age": 26,
    "last_modified": "2024-08-11T14:31:00.5678"
  }
}
```

**Failed Condition:**
```json
{
  "id": "req-456",
  "success": false,
  "error": "CONDITION_FAILED",
  "operation": "upsert",
  "table": "users",
  "condition_met": false,
  "expected_condition": "last_modified = '2024-08-11 14:30:00.1234'",
  "current_data": {
    "id": 123,
    "name": "Jane Doe",
    "age": 25,
    "last_modified": "2024-08-11T14:31:15.9999"
  },
  "message": "Record was modified by another user"
}
```

### **Optimistic Locking Workflow**

**1. Client lädt Daten:**
```sql
get users where id = 123
-- Response: {id: 123, name: "John", age: 25, last_modified: "14:30:00.1234"}
```

**2. User editiert lokal:**
```javascript
// User ändert name zu "John Smith"
const editedUser = {
  id: 123, 
  name: "John Smith", 
  age: 25,
  last_modified: "14:30:00.1234"  // Original timestamp behalten
};
```

**3. Conditional Save:**
```sql
upsert users {id: 123, name: "John Smith", age: 25} 
  if last_modified = "14:30:00.1234"
```

**4a. Success Case:**
```json
{
  "success": true,
  "data": {
    "id": 123,
    "name": "John Smith",
    "age": 25,
    "last_modified": "14:32:00.7777"  // Neuer timestamp
  }
}
```

**4b. Conflict Case:**
```json
{
  "success": false,
  "error": "CONDITION_FAILED",
  "current_data": {
    "id": 123,
    "name": "John Doe",        // Anderer User hat name geändert
    "age": 26,                 // Anderer User hat age geändert  
    "last_modified": "14:31:30.5555"
  }
}
```

**5. Conflict Resolution:**
```javascript
// Option A: Reload und User entscheiden lassen
// Option B: Auto-merge non-conflicting fields
// Option C: Force override mit normalem upsert
```

### **Implementation Details**

**Condition Evaluation:**
- Conditions werden gegen aktuellen DB-Stand evaluiert
- Unterstützt alle normalen Where-Operatoren: `=`, `!=`, `>`, `<`, `in`, etc.
- Multiple Conditions mit `and`/`or` möglich

**Atomic Operations:**
- Check + Write ist atomare Operation  
- Keine Race Conditions zwischen Condition-Check und Write
- Nutzt bestehende Branch-Queue für Atomicity

**Performance:**
- Condition-Check ist Teil der normalen Query-Execution
- Kein zusätzlicher DB-Zugriff nötig
- Bei failed condition: Kein Write, sofortiger Return

### **Use Cases**

1. **Multi-User Forms:** Verhindert versehentliches Überschreiben bei gleichzeitiger Bearbeitung
2. **Inventory Management:** Stock-Updates nur wenn Current-Stock = Expected-Stock
3. **Status Workflows:** State-Transitions nur von bestimmten Ausgangszuständen
4. **Financial Operations:** Geld-Transfers nur wenn Account-Balance ausreichend
5. **Version Control:** Document-Updates nur mit korrekter Version-Number

### **Error Handling**

**Invalid Conditions:**
```json
{
  "success": false,
  "error": "INVALID_CONDITION",
  "message": "Condition references non-existent field 'invalid_field'"
}
```

**Permission Errors:**
```json
{
  "success": false, 
  "error": "PERMISSION_DENIED",
  "message": "No read permission for condition evaluation on table 'users'"
}
```

---

## 🚀 **Implementation Priority**

### **Phase 1: Conditional Writes (User Benefit: 8/10)**
- Löst kritisches Multi-User-Concurrency Problem
- Einfache Syntax-Erweiterung
- Baut auf bestehender Query-Engine auf
- Sofort nutzbar für alle Multi-User-Szenarien

### **Phase 2: Query Macros (User Benefit: 7/10)**
- Massiver Developer Experience Boost
- Reduziert repetitive Arbeit erheblich
- Standardisiert Team-Workflows
- Erweiterbar für zukünftige Hook-Features

### **Combined Benefits**
Beide Features zusammen ermöglichen:
- **Sichere Multi-User-Anwendungen** (Conditional Writes)
- **Effiziente Entwicklungs-Workflows** (Macros)
- **Standardisierte Team-Prozesse** (Macros für Setup/Cleanup)
- **Reduzierte Fehlerquote** (Atomic Macro-Execution)

---

## 📝 **Syntax Summary**

```sql
-- Query Macros
create macro customer_setup(name) as ["query1", "query2"]
@customer_setup("acme")
get macros
get macro customer_setup
drop macro customer_setup

-- Conditional Writes  
upsert users {id: 123, name: "John"} if last_modified = "14:30:00.1234"
delete users where id = 456 if status = "pending"
```

Beide Features integrieren sich nahtlos in die bestehende SpoutDB-Syntax und -Architektur.

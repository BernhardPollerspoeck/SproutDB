# SpoutDB C# SDK Design

## 🎯 **Core Architecture**

### **Connection Management**
```csharp
// Server Connection with Database Selection
ServerConnection server = new("mydb://pat:token@server:8080/mycompany");

// Multi-Branch Selection (parallel connections possible)
BranchConnection main = await server.SelectBranchAsync("main");
BranchConnection feature = await server.SelectBranchAsync("feature/pricing");
BranchConnection tenant = await server.SelectBranchAsync("customer-a/prod");
```

### **Branch Management with Live Updates**
```csharp
// Observable Collection for UI Binding
ObservableCollection<string> branches = server.Branches;
// → Auto-updates: ["main", "feature/pricing", "customer-a/prod"]

// Events for Branch Changes
server.Branches.CollectionChanged += OnBranchesChanged;
// → Automatically fired when branches are created/deleted on server
```

---

## 📋 **Query Operations**

### **Query Result Types**
```csharp
public class QueryResult
{
    public string Branch { get; set; }
    public string? Commit { get; set; }      
    public DateTime Timestamp { get; set; }  
    public bool Success { get; set; }        
    public string? Error { get; set; }       
    public int? RowsScanned { get; set; }    
    public int? RowsReturned { get; set; }   
    public bool AutoPaginated { get; set; }  
    public string? NextPage { get; set; }    
}

public class QueryResult<T> : QueryResult
{
    public T Data { get; set; }              // Query result data
}
```

### **Read Operations**
```csharp
// Single object queries
QueryResult<User> user = await main.QueryAsync<User>("get users where id = 123", cancellationToken);

// List queries  
QueryResult<List<User>> users = await main.QueryAsync<List<User>>("get users where age > 25", cancellationToken);

// Complex queries with joins
QueryResult<List<UserOrder>> data = await main.QueryAsync<List<UserOrder>>(
    "get users follow users.id -> orders.user_id as orders select users.name, orders.total",
    cancellationToken
);

// Pagination
QueryResult<List<User>> page = await main.QueryAsync<List<User>>(
    "get users where city = 'Berlin' page 1 of size 100",
    cancellationToken
);
```

### **Write Operations**
```csharp
// Single upsert
QueryResult<User> result = await main.QueryAsync<User>(
    "upsert users {name: 'John', age: 25}",
    cancellationToken
);

// Bulk upsert  
QueryResult<List<User>> results = await main.QueryAsync<List<User>>(
    "upsert users [{name: 'John', age: 25}, {name: 'Jane', age: 30}] on email",
    cancellationToken
);

// Delete operations
QueryResult<int> deleteCount = await main.QueryAsync<int>(
    "delete users where last_login before 1 year",
    cancellationToken
);
```

### **Schema Operations (Non-Generic Results)**
```csharp
// Schema operations return non-generic QueryResult (no .Data property)
QueryResult schema = await main.QueryAsync(
    "add column users.premium boolean",
    cancellationToken
);

QueryResult expansion = await main.QueryAsync(
    "add column users.age mixed",  // Expands existing number column
    cancellationToken
);

QueryResult purge = await main.QueryAsync(
    "purge column users.old_field",
    cancellationToken
);

if (schema.Success)
{
    Logger.LogInformation($"Schema updated on {schema.Branch} at {schema.Timestamp}");
}
```

---

## 🔄 **Real-Time Subscriptions**

### **Live Data Streaming (Async Callbacks)**
```csharp
// Subscribe to data changes with async callbacks
await main.SubscribeAsync(
    "subscribe to orders where status = 'paid'", 
    OnOrderPaidAsync,
    cancellationToken
);

// Async subscription callback
private async Task OnOrderPaidAsync(QueryResult<Order> orderUpdate)
{
    if (orderUpdate.Success)
    {
        Console.WriteLine($"New paid order: {orderUpdate.Data.Id}");
        await UpdateDashboardAsync(orderUpdate.Data);
        await NotifyTeamsAsync(orderUpdate.Data);
    }
}

// Unsubscribe with cancellation
await main.UnsubscribeAsync("subscribe to orders where status = 'paid'", cancellationToken);
```

### **Subscription Management**
```csharp
// Subscriptions are automatically managed by BranchConnection
// Auto-cleanup on BranchConnection.Dispose()

// Manual unsubscribe (if needed)
await main.UnsubscribeAsync("subscribe to orders where status = 'paid'");

// Dispose pattern
using var main = await server.SelectBranchAsync("main");
await main.SubscribeAsync("subscribe to orders", callback);
// → Auto-unsubscribe on dispose
```

---

## 🌿 **Branch Operations**

### **Branch Management (Non-Generic Operations)**
```csharp
// Branch operations return non-generic QueryResult
QueryResult createResult = await server.QueryAsync("create branch feature/new-pricing from main", cancellationToken);

QueryResult checkoutResult = await server.QueryAsync(
    "checkout branch main as of '2024-08-13 14:30' as debug-snapshot",
    cancellationToken
);

QueryResult mergeResult = await server.QueryAsync("merge feature/new-pricing into main", cancellationToken);

QueryResult deleteResult = await server.QueryAsync("delete branch feature/old-feature", cancellationToken);

// Check success without .Data property
if (createResult.Success)
{
    Logger.LogInformation($"Branch created: {createResult.Commit}");
}
```

### **Branch Aliases (Non-Generic Operations)**
```csharp
// Alias operations return non-generic QueryResult
QueryResult aliasResult = await server.QueryAsync("create alias prod-stable for branch main", cancellationToken);

// Use alias in branch selection
BranchConnection stable = await server.SelectBranchAsync("prod-stable", cancellationToken);

QueryResult updateResult = await server.QueryAsync("update alias prod-stable to branch release/v2.1", cancellationToken);
```

---

## ⚡ **Performance & Reliability**

### **Error Handling**
```csharp
QueryResult<List<User>> result = await main.QueryAsync<List<User>>("get users", cancellationToken);

if (result.Success)
{
    ProcessUsers(result.Data);
    Logger.LogInformation($"Query executed on {result.Branch} at {result.Timestamp}");
}
else
{
    Logger.LogError($"Query failed: {result.Error}");
    HandleQueryError(result.Error);
}
```

### **Health Monitoring**
```csharp
// Server health check
bool serverHealthy = await server.PingAsync(cancellationToken);

// Branch-specific health (checks if branch is accessible)
bool branchHealthy = await main.PingAsync(cancellationToken);

// Health check with timeout via CancellationToken
using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
bool quickCheck = await server.PingAsync(cts.Token);

// Use in health check endpoints
[HttpGet("/health")]
public async Task<IActionResult> HealthCheck(CancellationToken cancellationToken)
{
    var healthy = await _server.PingAsync(cancellationToken);
    return healthy ? Ok() : StatusCode(503);
}
```

### **Auto-Pagination Handling**
```csharp
// Server health check
bool serverHealthy = await server.PingAsync();

// Branch-specific health (checks if branch is accessible)
bool branchHealthy = await main.PingAsync();

// Health check with timeout
bool quickCheck = await server.PingAsync(TimeSpan.FromSeconds(5));

// Use in health check endpoints
[HttpGet("/health")]
public async Task<IActionResult> HealthCheck()
{
    var healthy = await _server.PingAsync();
    return healthy ? Ok() : StatusCode(503);
}
```
```csharp
QueryResult<List<User>> result = await main.QueryAsync<List<User>>(
    "get users where city = 'Berlin'"
);

if (result.AutoPaginated)
{
    Logger.LogInformation($"Auto-paginated: {result.NextPage}");
    
    // Get next page
    var nextPage = await main.QueryAsync<List<User>>(result.NextPage);
}
```

### **Replica Failover (Transparent)**
```csharp
// SDK handles replica failover automatically via ILogger
private readonly ILogger<ServerConnection> _logger;

// Internal failover logging
_logger.LogInformation("Primary {OldPrimary} failed, switched to {NewPrimary}", 
    oldPrimary, newPrimary);

// Developer code continues working without interruption
var users = await main.QueryAsync<List<User>>("get users");
// → Works even during primary failover
```

---

## 🔧 **Configuration & Setup**

### **Connection String Format & Options**
```
mydb://[pat:token@]server[:port]/database[?options]
```

**Examples:**
```csharp
// Local development (no auth)
new ServerConnection("mydb://localhost:8080/testdb");

// Production with PAT
new ServerConnection("mydb://pat:abc123def@prod.server.com:8080/mycompany");

// With connection options
new ServerConnection("mydb://pat:token@server:8080/db?timeout=30000&retries=3&keepalive=true");
```

**Available Options:**
- `timeout=30000` - Query timeout in milliseconds (default: 30000)
- `retries=3` - Connection retry attempts (default: 3)
- `keepalive=true` - SignalR keepalive (default: true)
- `compression=true` - Enable response compression (default: true)
- `loglevel=info` - Client-side log level (default: info)

### **Dependency Injection Setup**
```csharp
// Program.cs
services.AddSpoutDb("mydb://pat:token@server:8080/mycompany");

// Or with options
services.AddSpoutDb("mydb://pat:token@server:8080/mycompany", options => {
    options.DefaultTimeout = TimeSpan.FromSeconds(30);
    options.MaxRetries = 3;
});

// Injected ISpoutDbConnectionFactory available for services
```

---

## 🎯 **Use Case Examples**

### **Multi-Tenant SaaS Application**
```csharp
// Core data on main branch
BranchConnection core = await server.SelectBranchAsync("main", cancellationToken);
var products = await core.QueryAsync<List<Product>>("get products where active = true", cancellationToken);

// Tenant-specific data
BranchConnection tenant = await server.SelectBranchAsync("customer-a/prod", cancellationToken);
var settings = await tenant.QueryAsync<TenantConfig>("get tenant_config", cancellationToken);

// Live updates for tenant
await tenant.SubscribeAsync("subscribe to orders where status = 'paid'", OnTenantOrder, cancellationToken);
```

### **Development with Feature Branches**
```csharp
// Production data
BranchConnection prod = await server.SelectBranchAsync("main", cancellationToken);

// Feature development
BranchConnection feature = await server.SelectBranchAsync("feature/new-pricing", cancellationToken);

// Compare results
var prodPricing = await prod.QueryAsync<List<Product>>("get products", cancellationToken);
var featurePricing = await feature.QueryAsync<List<Product>>("get products", cancellationToken);

// Safe testing without affecting production
await feature.QueryAsync<Product>("upsert products {id: 1, price: 99.99}", cancellationToken);
```

### **Time Travel Debugging**
```csharp
// Current state
var currentUsers = await main.QueryAsync<List<User>>("get users", cancellationToken);

// Create debug snapshot
await server.QueryAsync(
    "checkout branch main as of '2024-08-13 14:30' as debug-snapshot",
    cancellationToken
);

// Debug with historical data
BranchConnection debug = await server.SelectBranchAsync("debug-snapshot", cancellationToken);
var historicalUsers = await debug.QueryAsync<List<User>>("get users", cancellationToken);

// Compare states for debugging
CompareStates(currentUsers.Data, historicalUsers.Data);
```

---

## 📦 **Package Structure**

```
SpoutDB.Client/
├── Connections/
│   ├── ServerConnection.cs
│   ├── BranchConnection.cs
│   └── ISpoutDbConnection.cs
├── Models/
│   ├── QueryResult.cs
│   ├── BranchInfo.cs
│   └── ConnectionOptions.cs
├── Exceptions/
│   ├── SpoutDbException.cs
│   ├── QueryException.cs
│   └── ConnectionException.cs
└── Extensions/
    ├── ServiceCollectionExtensions.cs
    └── ConfigurationExtensions.cs
```

**Target Framework:** .NET 9 (for latest performance optimizations and modern async patterns)

**Dependencies:**
- Microsoft.AspNetCore.SignalR.Client (for real-time communication)
- Microsoft.Extensions.Logging.Abstractions (for logging)
- System.Collections.ObjectModel (for ObservableCollection)

---

## ✅ **Implementation Roadmap**

### **Phase 1 - Core SDK (2 Months)**
- ServerConnection & BranchConnection
- Basic QueryAsync<T> with QueryResult<T>
- Connection string parsing
- Error handling

### **Phase 2 - Real-Time Features (1 Month)**  
- SignalR integration
- SubscribeAsync functionality
- Branch management (Observable Collections)
- Replica failover handling

### **Phase 3 - Advanced Features (1 Month)**
- Time travel operations
- Branch aliases support  
- Auto-pagination handling
- Performance optimizations

### **Phase 4 - Production Ready (1 Month)**
- Comprehensive error handling
- Dependency injection support
- Unit tests & integration tests
- Documentation & examples

**Total Development Time:** ~5 Months → Production-Ready C# SDK
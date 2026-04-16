using System.Buffers.Binary;
using System.Reflection;
using System.Threading.Channels;
using SproutDB.Core.Auth;
using SproutDB.Core.AutoIndex;
using SproutDB.Core.Execution;
using SproutDB.Core.Parsing;
using SproutDB.Core.Storage;

namespace SproutDB.Core;

/// <summary>
/// Main entry point for the SproutDB engine.
/// Single-writer queue (Channel) serializes all mutations.
/// Reads (GetQuery) run lock-free on the caller thread against MMFs.
/// Background: WAL group commit (fsync), flush cycle (MMF flush + WAL truncate).
/// </summary>
public sealed class SproutEngine : ISproutServer, IDisposable
{
    private readonly string _dataDirectory;
    private readonly SproutEngineSettings _settings;
    private readonly TableCache _tableCache;
    private readonly WalManager _walManager = new();
    private readonly DatabaseScopeManager _scopes;
    private readonly IndexMetricsStore _indexMetrics = new();
    private readonly SproutAuthService? _authService;
    private readonly SproutChangeNotifier _changeNotifier = new();

    // Writer channel
    private readonly Channel<Action> _writeChannel;
    private readonly Task _writerTask;

    // Background cycles
    private readonly Task _flushTask;
    private readonly Task _walSyncTask;
    private readonly Task _ttlCleanupTask;
    private readonly TimeSpan _walSyncInterval;
    private readonly CancellationTokenSource _disposeCts = new();
    private volatile bool _disposed;

    // Monitoring counters
    private readonly DateTime _startedAtUtc = DateTime.UtcNow;
    private long _totalReads;
    private long _totalWrites;

    /// <summary>
    /// Creates a new SproutDB engine with default settings.
    /// </summary>
    public SproutEngine(string dataDirectory)
        : this(new SproutEngineSettings { DataDirectory = dataDirectory })
    {
    }

    /// <summary>
    /// Creates a new SproutDB engine with the specified settings.
    /// </summary>
    public SproutEngine(SproutEngineSettings settings)
        : this(settings, null)
    {
    }

    /// <summary>
    /// Creates a new SproutDB engine with settings and optional auth configuration.
    /// Used by DI when <see cref="SproutAuthOptions"/> is registered.
    /// </summary>
    public SproutEngine(SproutEngineSettings settings, SproutAuthOptions? authOptions)
    {
        // Merge MasterKey from auth options into settings if not already set
        if (authOptions is not null && settings.MasterKey is null)
        {
            settings = new SproutEngineSettings
            {
                DataDirectory = settings.DataDirectory,
                FlushInterval = settings.FlushInterval,
                WalSyncInterval = settings.WalSyncInterval,
                BulkLimit = settings.BulkLimit,
                DefaultPageSize = settings.DefaultPageSize,
                ChunkSize = settings.ChunkSize,
                AutoIndex = settings.AutoIndex,
                MasterKey = authOptions.MasterKey,
                TtlCleanupInterval = settings.TtlCleanupInterval,
                TtlCleanupBatchSize = settings.TtlCleanupBatchSize,
                IdleEvictAfterSeconds = settings.IdleEvictAfterSeconds,
                MaxOpenDatabases = settings.MaxOpenDatabases,
                EnableMemoryPressureEviction = settings.EnableMemoryPressureEviction,
                MemoryPressureThresholdPercent = settings.MemoryPressureThresholdPercent,
                IdleEvictInterval = settings.IdleEvictInterval,
            };
        }

        _settings = settings;
        _dataDirectory = Path.GetFullPath(settings.DataDirectory);
        _tableCache = new TableCache(settings.ChunkSize);
        _scopes = new DatabaseScopeManager(_walManager, _tableCache, settings);
        Directory.CreateDirectory(_dataDirectory);

        _walSyncInterval = settings.WalSyncInterval;

        // ── Startup replay: discover all databases, replay WALs ──
        ReplayAllDatabases();

        // ── Repair B-Trees that may have duplicate entries (one-time) ──
        RepairBTreesIfNeeded();

        // ── Ensure _system database exists ────────────────────────
        EnsureSystemDatabase();
        _scopes.Pin(Path.Combine(_dataDirectory, "_system"));
        LoadIndexMetrics();

        // ── Initialize auth if configured ────────────────────────
        if (settings.MasterKey is not null)
        {
            _authService = new SproutAuthService();
            _authService.Initialize(this, settings.MasterKey);
        }

        // ── Start writer channel ─────────────────────────────────
        _writeChannel = Channel.CreateUnbounded<Action>(new UnboundedChannelOptions
        {
            SingleReader = true,
        });
        _writerTask = Task.Factory.StartNew(
            RunWriterLoop,
            CancellationToken.None,
            TaskCreationOptions.LongRunning,
            TaskScheduler.Default).Unwrap();

        // ── Start background cycles ──────────────────────────────
        _flushTask = RunFlushCycle(settings.FlushInterval);
        _walSyncTask = RunWalSyncCycle(_walSyncInterval);
        _ttlCleanupTask = RunTtlCleanupCycle(settings.TtlCleanupInterval);
    }

    /// <summary>
    /// Auth service instance (null if auth is not configured).
    /// </summary>
    internal SproutAuthService? AuthService => _authService;

    /// <summary>
    /// Engine settings (needed by endpoint layer for MasterKey, BulkLimit).
    /// </summary>
    internal SproutEngineSettings Settings => _settings;

    /// <summary>
    /// Change notification service for real-time events.
    /// </summary>
    internal SproutChangeNotifier ChangeNotifier => _changeNotifier;

    /// <summary>
    /// Executes one or more queries (semicolon-separated) against the specified database.
    /// Returns one SproutResponse per query/transaction.
    /// </summary>
    public List<SproutResponse> Execute(string query, string database)
    {
        var dbName = LowercaseName(database);

        if (!IsValidName(dbName))
            return [ResponseHelper.Error(query, ErrorCodes.SYNTAX_ERROR,
                $"invalid database name '{database}'")];

        var dbPath = Path.Combine(_dataDirectory, dbName);

        // Parse all queries on caller thread (stateless)
        var parseResults = QueryParser.ParseMulti(query);
        var responses = new List<SproutResponse>(parseResults.Count);

        // One lease covers the whole call — blocks eviction of this DB while
        // any of these queries (incl. queued writes) are in flight.
        using var lease = _scopes.Acquire(dbPath);

        foreach (var parseResult in parseResults)
        {
            if (!parseResult.Success)
            {
                responses.Add(ResponseHelper.ParseError(parseResult));
                continue;
            }

            var parsedQuery = parseResult.Query;
            if (parsedQuery is null)
            {
                responses.Add(ResponseHelper.ParseError(parseResult));
                continue;
            }

            // Transaction returns multiple responses
            if (parsedQuery is TransactionQuery txQuery)
            {
                var txResponses = PostWriteList(() => ExecuteTransaction(query, dbName, dbPath, txQuery));
                responses.AddRange(txResponses);
            }
            else
            {
                responses.Add(ExecuteSingle(query, dbName, dbPath, parsedQuery));
            }
        }

        return responses;
    }

    /// <summary>
    /// Routes and executes a single parsed query (or transaction block).
    /// </summary>
    private SproutResponse ExecuteSingle(string query, string dbName, string dbPath, IQuery parsedQuery)
    {
        // Auth queries bypass normal routing and write protection
        if (SproutAuthService.IsAuthQuery(parsedQuery))
            return PostWrite(() => ExecuteAuthQuery(query, parsedQuery));

        // Reads bypass the channel → run directly on caller thread
        if (parsedQuery is GetQuery getQuery)
            return ExecuteGet(query, dbName, dbPath, getQuery);

        if (parsedQuery is DescribeQuery describeQuery)
            return ExecuteDescribe(query, dbName, dbPath, describeQuery);

        // Write protection: block writes to _-prefixed system entities
        var protectionError = CheckWriteProtection(query, parsedQuery, dbName);
        if (protectionError is not null)
            return protectionError;

        // All mutations go through the writer channel
        return PostWrite(() => ExecuteWrite(query, dbName, dbPath, parsedQuery));
    }

    /// <summary>
    /// Executes a query internally, bypassing _ prefix write protection.
    /// Used for system writes (_system, _migrations).
    /// </summary>
    internal SproutResponse ExecuteInternal(string query, string database)
    {
        var dbName = LowercaseName(database);

        if (!IsValidName(dbName))
            return ResponseHelper.Error(query, ErrorCodes.SYNTAX_ERROR,
                $"invalid database name '{database}'");

        var dbPath = Path.Combine(_dataDirectory, dbName);

        using var lease = _scopes.Acquire(dbPath);

        var parseResult = QueryParser.Parse(query);
        if (!parseResult.Success)
            return ResponseHelper.ParseError(parseResult);

        var parsedQuery = parseResult.Query;
        if (parsedQuery is null)
            return ResponseHelper.ParseError(parseResult);

        if (parsedQuery is GetQuery getQuery)
            return ExecuteGet(query, dbName, dbPath, getQuery);

        if (parsedQuery is DescribeQuery describeQuery)
            return ExecuteDescribe(query, dbName, dbPath, describeQuery);

        // No protection check — internal writes bypass protection
        return PostWrite(() => ExecuteWrite(query, dbName, dbPath, parsedQuery));
    }

    // ── Writer channel ──────────────────────────────────────

    private SproutResponse PostWrite(Func<SproutResponse> work)
    {
        var tcs = new TaskCompletionSource<SproutResponse>(TaskCreationOptions.RunContinuationsAsynchronously);

        var action = new Action(tcs, work);
        if (!_writeChannel.Writer.TryWrite(action))
            return ResponseHelper.Error("", ErrorCodes.SYNTAX_ERROR, "engine is shutting down");

        return tcs.Task.GetAwaiter().GetResult();
    }

    private List<SproutResponse> PostWriteList(Func<List<SproutResponse>> work)
    {
        var tcs = new TaskCompletionSource<SproutResponse>(TaskCreationOptions.RunContinuationsAsynchronously);
        List<SproutResponse>? listResult = null;

        var action = new Action(tcs, () =>
        {
            listResult = work();
            return new SproutResponse { Operation = SproutOperation.Transaction }; // dummy
        });

        if (!_writeChannel.Writer.TryWrite(action))
            return [ResponseHelper.Error("", ErrorCodes.SYNTAX_ERROR, "engine is shutting down")];

        tcs.Task.GetAwaiter().GetResult();
        return listResult ?? [];
    }

    private async Task RunWriterLoop()
    {
        await foreach (var action in _writeChannel.Reader.ReadAllAsync())
        {
            try
            {
                var result = action.Work();
                action.Completion.TrySetResult(result);
            }
            catch (Exception ex)
            {
                action.Completion.TrySetException(ex);
            }
        }
    }

    /// <summary>
    /// Executes a mutating query on the writer thread.
    /// </summary>
    private SproutResponse ExecuteWrite(string query, string dbName, string dbPath, IQuery parsedQuery)
    {
        if (parsedQuery is CreateDatabaseQuery cdbq)
        {
            var r = ExecuteCreateDatabase(query, dbName, dbPath, cdbq);
            if (r.Errors is null)
            {
                LogAudit(dbName, query, "create_database");
                _changeNotifier.Enqueue(dbName, "_schema", r);
            }
            return r;
        }

        if (parsedQuery is PurgeDatabaseQuery)
        {
            var r = ExecutePurgeDatabase(query, dbName, dbPath);
            if (r.Errors is null)
            {
                LogAudit(dbName, query, "purge_database");
                _changeNotifier.Enqueue(dbName, "_schema", r);
            }
            return r;
        }

        if (parsedQuery is BackupQuery)
            return ExecuteBackup(query, dbName, dbPath);

        if (parsedQuery is RestoreQuery restoreQ)
            return ExecuteRestore(query, dbName, dbPath, restoreQ);

        // All other mutations require the database to exist
        if (!_tableCache.DatabaseExists(dbPath))
            return ResponseHelper.Error(query, ErrorCodes.UNKNOWN_DATABASE,
                $"database '{dbName}' does not exist");

        // WAL append before mutation
        ulong resolvedId = ResolveIdForWal(parsedQuery, dbPath);
        var wal = _walManager.GetOrOpen(dbPath);
        wal.Append(query, resolvedId);

        // Immediate fsync if group commit is disabled
        if (_walSyncInterval == TimeSpan.Zero)
            wal.SyncToDisk();

        var result = Dispatch(query, dbName, dbPath, parsedQuery);

        // Track write metrics for index columns
        if (result.Errors is null)
        {
            Interlocked.Increment(ref _totalWrites);

            if (parsedQuery is UpsertQuery uq)
            {
                var tablePath = Path.Combine(dbPath, uq.Table);
                foreach (var rec in uq.Records)
                    foreach (var f in rec)
                        _indexMetrics.RecordWrite(tablePath, f.Name);
            }
            else if (parsedQuery is CreateIndexQuery ciq)
            {
                var tablePath = Path.Combine(dbPath, ciq.Table);
                _indexMetrics.MarkManual(tablePath, ciq.Column);
                var manualMetrics = _indexMetrics.GetOrCreate(tablePath, ciq.Column);
                manualMetrics.IndexCreatedAt ??= DateTime.UtcNow;
            }

            // Audit log for schema changes
            var operation = GetSchemaOperation(parsedQuery);
            if (operation is not null)
                LogAudit(dbName, query, operation);

            // Change notifications (non-blocking enqueue)
            var tableName = GetTableNameForNotify(parsedQuery);
            if (tableName is not null)
            {
                _changeNotifier.Enqueue(dbName, tableName, result);
                if (operation is not null)
                    _changeNotifier.Enqueue(dbName, "_schema", result);
            }
        }

        return result;
    }

    /// <summary>
    /// Executes an atomic transaction block on the writer thread.
    /// All writes succeed or all are rolled back (MMF evict + reopen).
    /// </summary>
    private List<SproutResponse> ExecuteTransaction(string query, string dbName, string dbPath, TransactionQuery txQuery)
    {
        if (!_tableCache.DatabaseExists(dbPath))
            return [ResponseHelper.Error(query, ErrorCodes.UNKNOWN_DATABASE,
                $"database '{dbName}' does not exist")];

        var wal = _walManager.GetOrOpen(dbPath);
        var groupId = wal.NextGroupId();
        var totalAffected = 0;
        var pendingNotifications = new List<(string Table, SproutResponse Response)>();
        var journal = new Storage.TransactionJournal();
        var results = new List<SproutResponse>(txQuery.Queries.Count + 1);

        // 1. Write protection check for write queries
        foreach (var innerQuery in txQuery.Queries)
        {
            if (innerQuery is GetQuery or DescribeQuery)
                continue;
            var protectionError = CheckWriteProtection(query, innerQuery, dbName);
            if (protectionError is not null)
                return [protectionError];
        }

        // 2. WAL append write queries with shared groupId (reads skip WAL)
        for (var idx = 0; idx < txQuery.Queries.Count; idx++)
        {
            var innerQuery = txQuery.Queries[idx];
            if (innerQuery is GetQuery or DescribeQuery)
                continue;
            var innerQueryText = txQuery.QueryTexts[idx];
            ulong resolvedId = ResolveIdForWal(innerQuery, dbPath);
            wal.Append(innerQueryText, resolvedId, groupId);
        }

        // 3. Execute all queries sequentially
        try
        {
            for (var idx = 0; idx < txQuery.Queries.Count; idx++)
            {
                var innerQuery = txQuery.Queries[idx];
                var innerQueryText = txQuery.QueryTexts[idx];

                // Reads execute directly (no journal needed, they don't mutate)
                SproutResponse result;
                if (innerQuery is GetQuery gq)
                {
                    result = ExecuteGet(innerQueryText, dbName, dbPath, gq);
                    Interlocked.Increment(ref _totalReads);
                }
                else if (innerQuery is DescribeQuery dq)
                {
                    result = ExecuteDescribe(innerQueryText, dbName, dbPath, dq);
                    Interlocked.Increment(ref _totalReads);
                }
                else
                {
                    result = Dispatch(innerQueryText, dbName, dbPath, innerQuery, journal);
                }

                if (result.Errors is not null && result.Errors.Count > 0)
                {
                    // Rollback: undo all MMF changes via journal, mark WAL group
                    journal.Rollback();
                    wal.MarkGroupRolledBack(groupId);
                    return [ResponseHelper.Error(query, result.Errors[0].Code,
                        $"transaction rolled back: {result.Errors[0].Message}")];
                }

                results.Add(result);
                totalAffected += result.Affected;

                // Collect notifications for dispatch after commit
                var tableName = GetTableNameForNotify(innerQuery);
                if (tableName is not null)
                    pendingNotifications.Add((tableName, result));

                // Track write metrics
                if (innerQuery is not (GetQuery or DescribeQuery))
                {
                    Interlocked.Increment(ref _totalWrites);
                    if (innerQuery is UpsertQuery uq)
                    {
                        var tablePath = Path.Combine(dbPath, uq.Table);
                        foreach (var rec in uq.Records)
                            foreach (var f in rec)
                                _indexMetrics.RecordWrite(tablePath, f.Name);
                    }
                }
            }
        }
        catch (Exception ex)
        {
            // Unexpected error — rollback via journal
            journal.Rollback();
            wal.MarkGroupRolledBack(groupId);
            return [ResponseHelper.Error(query, ErrorCodes.SYNTAX_ERROR,
                $"transaction rolled back: {ex.Message}")];
        }

        // 4. Success — flush WAL
        if (_walSyncInterval == TimeSpan.Zero)
            wal.SyncToDisk();

        // 5. Dispatch change notifications
        foreach (var (table, response) in pendingNotifications)
            _changeNotifier.Enqueue(dbName, table, response);

        // 6. Audit log
        LogAudit(dbName, query, "transaction");

        // 7. Append Transaction marker as last entry
        results.Add(new SproutResponse
        {
            Operation = SproutOperation.Transaction,
            Affected = totalAffected,
        });

        return results;
    }

    // ── Startup replay ──────────────────────────────────────

    private void ReplayAllDatabases()
    {
        if (!Directory.Exists(_dataDirectory))
            return;

        foreach (var dbDir in Directory.GetDirectories(_dataDirectory))
        {
            var dbName = Path.GetFileName(dbDir);
            _tableCache.RegisterDatabase(dbDir);

            // Open all existing tables for this database
            _tableCache.OpenTablesForDatabase(dbDir);

            // Replay WAL if present
            ReplayWal(dbDir, dbName);
        }
    }

    /// <summary>
    /// One-time repair: rebuild all B-Trees to remove duplicate entries
    /// caused by a bug in BTreeHandle.Remove with duplicate keys.
    /// Uses a version marker file so this only runs once per data directory.
    /// </summary>
    private void RepairBTreesIfNeeded()
    {
        var versionFile = Path.Combine(_dataDirectory, "_btree_version");
        const int requiredVersion = 2;

        if (File.Exists(versionFile))
        {
            var content = File.ReadAllText(versionFile).Trim();
            if (int.TryParse(content, out var version) && version >= requiredVersion)
                return;
        }

        foreach (var (_, table) in _tableCache.GetAllOpened())
        {
            if (table.IndexCount > 0)
                table.RebuildAllBTrees();
        }

        File.WriteAllText(versionFile, requiredVersion.ToString());
    }

    // ── System database ────────────────────────────────────

    private void EnsureSystemDatabase()
    {
        var systemDbPath = Path.Combine(_dataDirectory, "_system");

        // Ensure database directory + _meta.bin
        if (!Directory.Exists(systemDbPath))
        {
            Directory.CreateDirectory(systemDbPath);
            MetaFile.Write(Path.Combine(systemDbPath, "_meta.bin"), DateTime.UtcNow.Ticks);
        }

        _tableCache.RegisterDatabase(systemDbPath);

        // Ensure audit_log table
        EnsureSystemTable(systemDbPath, "audit_log",
        [
            new() { Name = "timestamp", Type = "datetime", Size = 8, EntrySize = 9, Nullable = false, Strict = true },
            new() { Name = "database", Type = "string", Size = 128, EntrySize = 129, Nullable = true },
            new() { Name = "query", Type = "string", Size = 1024, EntrySize = 1025, Nullable = false },
            new() { Name = "operation", Type = "string", Size = 64, EntrySize = 65, Nullable = false, Strict = true },
        ]);

        // Ensure _api_keys table
        EnsureSystemTable(systemDbPath, "_api_keys",
        [
            new() { Name = "name", Type = "string", Size = 128, EntrySize = 129, Nullable = false, Strict = true },
            new() { Name = "key_prefix", Type = "string", Size = 16, EntrySize = 17, Nullable = false, Strict = true },
            new() { Name = "key_hash", Type = "string", Size = 128, EntrySize = 129, Nullable = false, Strict = true },
            new() { Name = "created_at", Type = "datetime", Size = 8, EntrySize = 9, Nullable = false },
            new() { Name = "last_used_at", Type = "datetime", Size = 8, EntrySize = 9, Nullable = true },
        ]);

        // Ensure _api_permissions table
        EnsureSystemTable(systemDbPath, "_api_permissions",
        [
            new() { Name = "key_name", Type = "string", Size = 128, EntrySize = 129, Nullable = false, Strict = true },
            new() { Name = "database", Type = "string", Size = 128, EntrySize = 129, Nullable = false, Strict = true },
            new() { Name = "role", Type = "string", Size = 16, EntrySize = 17, Nullable = false, Strict = true },
        ]);

        // Ensure _api_restrictions table
        EnsureSystemTable(systemDbPath, "_api_restrictions",
        [
            new() { Name = "key_name", Type = "string", Size = 128, EntrySize = 129, Nullable = false, Strict = true },
            new() { Name = "database", Type = "string", Size = 128, EntrySize = 129, Nullable = false, Strict = true },
            new() { Name = "table", Type = "string", Size = 128, EntrySize = 129, Nullable = false, Strict = true },
            new() { Name = "role", Type = "string", Size = 16, EntrySize = 17, Nullable = false, Strict = true },
        ]);

        // Ensure index_metrics table
        EnsureSystemTable(systemDbPath, "index_metrics",
        [
            new() { Name = "key", Type = "string", Size = 640, EntrySize = 641, Nullable = false, Strict = true },
            new() { Name = "table_name", Type = "string", Size = 128, EntrySize = 129, Nullable = false },
            new() { Name = "column_name", Type = "string", Size = 128, EntrySize = 129, Nullable = false },
            new() { Name = "query_count", Type = "slong", Size = 8, EntrySize = 9, Nullable = false, Default = "0" },
            new() { Name = "where_hit_count", Type = "slong", Size = 8, EntrySize = 9, Nullable = false, Default = "0" },
            new() { Name = "read_count", Type = "slong", Size = 8, EntrySize = 9, Nullable = false, Default = "0" },
            new() { Name = "write_count", Type = "slong", Size = 8, EntrySize = 9, Nullable = false, Default = "0" },
            new() { Name = "scanned_total", Type = "slong", Size = 8, EntrySize = 9, Nullable = false, Default = "0" },
            new() { Name = "result_total", Type = "slong", Size = 8, EntrySize = 9, Nullable = false, Default = "0" },
            new() { Name = "is_manual", Type = "bool", Size = 1, EntrySize = 2, Nullable = false, Default = "false" },
            new() { Name = "last_used_at", Type = "datetime", Size = 8, EntrySize = 9, Nullable = true },
            new() { Name = "index_created_at", Type = "datetime", Size = 8, EntrySize = 9, Nullable = true },
        ]);
    }

    private void EnsureSystemTable(string dbPath, string tableName, List<ColumnSchemaEntry> columns)
    {
        var tablePath = Path.Combine(dbPath, tableName);
        var schemaPath = Path.Combine(tablePath, "_schema.bin");

        if (Directory.Exists(tablePath) && File.Exists(schemaPath))
        {
            // Already exists — just ensure it's in the cache
            _tableCache.GetOrOpen(tablePath);
            return;
        }

        // Directory may exist without schema (partial/failed creation) — recreate all files
        Directory.CreateDirectory(tablePath);

        var schema = new TableSchema
        {
            CreatedTicks = DateTime.UtcNow.Ticks,
            Columns = columns,
        };

        SchemaFile.Write(Path.Combine(tablePath, "_schema.bin"), schema);

        // Create _index file (slot-based format with header)
        var indexPath = Path.Combine(tablePath, "_index");
        IndexHandle.CreateNew(indexPath, _settings.ChunkSize);

        // Create .col files
        foreach (var col in columns)
        {
            using var fs = File.Create(Path.Combine(tablePath, $"{col.Name}.col"));
            fs.SetLength((long)_settings.ChunkSize * col.EntrySize);
        }

        _tableCache.GetOrOpen(tablePath);
    }

    // ── Auth query execution ────────────────────────────────

    private SproutResponse ExecuteAuthQuery(string query, IQuery parsedQuery)
    {
        var systemDbPath = Path.Combine(_dataDirectory, "_system");

        var apiKeysPath = Path.Combine(systemDbPath, "_api_keys");
        var apiPermissionsPath = Path.Combine(systemDbPath, "_api_permissions");
        var apiRestrictionsPath = Path.Combine(systemDbPath, "_api_restrictions");

        if (!_tableCache.TryGetTable(apiKeysPath, out var apiKeysTable) || apiKeysTable is null)
            return ResponseHelper.Error(query, ErrorCodes.SYNTAX_ERROR, "auth system tables not initialized");

        if (!_tableCache.TryGetTable(apiPermissionsPath, out var apiPermissionsTable) || apiPermissionsTable is null)
            return ResponseHelper.Error(query, ErrorCodes.SYNTAX_ERROR, "auth system tables not initialized");

        if (!_tableCache.TryGetTable(apiRestrictionsPath, out var apiRestrictionsTable) || apiRestrictionsTable is null)
            return ResponseHelper.Error(query, ErrorCodes.SYNTAX_ERROR, "auth system tables not initialized");

        if (_authService is null)
            return ResponseHelper.Error(query, ErrorCodes.SYNTAX_ERROR, "auth is not configured");

        var result = parsedQuery switch
        {
            CreateApiKeyQuery q => CreateApiKeyExecutor.Execute(query, q, apiKeysTable, _authService, _settings.BulkLimit),
            PurgeApiKeyQuery q => PurgeApiKeyExecutor.Execute(query, q, apiKeysTable, apiPermissionsTable, apiRestrictionsTable, _authService, _settings.BulkLimit),
            RotateApiKeyQuery q => RotateApiKeyExecutor.Execute(query, q, apiKeysTable, _authService, _settings.BulkLimit),
            GrantQuery q => GrantExecutor.Execute(query, q, apiPermissionsTable, _authService, _settings.BulkLimit),
            RevokeQuery q => RevokeExecutor.Execute(query, q, apiPermissionsTable, apiRestrictionsTable, _authService, _settings.BulkLimit),
            RestrictQuery q => RestrictExecutor.Execute(query, q, apiRestrictionsTable, _authService, _settings.BulkLimit),
            UnrestrictQuery q => UnrestrictExecutor.Execute(query, q, apiRestrictionsTable, _authService, _settings.BulkLimit),
            _ => ResponseHelper.Error(query, ErrorCodes.SYNTAX_ERROR, "unknown auth operation"),
        };

        if (result.Errors is null)
            LogAudit(null, query, GetAuthOperation(parsedQuery) ?? "auth");

        return result;
    }

    private static string? GetAuthOperation(IQuery query) => query switch
    {
        CreateApiKeyQuery => "create_apikey",
        PurgeApiKeyQuery => "purge_apikey",
        RotateApiKeyQuery => "rotate_apikey",
        GrantQuery => "grant",
        RevokeQuery => "revoke",
        RestrictQuery => "restrict",
        UnrestrictQuery => "unrestrict",
        _ => null,
    };

    // ── Audit logging ────────────────────────────────────────

    private void LogAudit(string? database, string query, string operation)
    {
        var systemDbPath = Path.Combine(_dataDirectory, "_system");
        var auditTablePath = Path.Combine(systemDbPath, "audit_log");

        if (!_tableCache.TryGetTable(auditTablePath, out var table) || table is null)
            return;

        var now = DateTime.UtcNow.ToString("yyyy-MM-dd HH:mm:ss");
        // Truncate query to max column size (1024)
        if (query.Length > 1024)
            query = query[..1024];

        var auditQuery = database is not null
            ? $"upsert audit_log {{timestamp: '{now}', database: '{EscapeString(database)}', query: '{EscapeString(query)}', operation: '{EscapeString(operation)}'}}"
            : $"upsert audit_log {{timestamp: '{now}', query: '{EscapeString(query)}', operation: '{EscapeString(operation)}'}}";

        var parseResult = QueryParser.Parse(auditQuery);
        if (parseResult.Success && parseResult.Query is UpsertQuery uq)
            UpsertExecutor.Execute(auditQuery, table, uq, _settings.BulkLimit);
    }

    private static string? GetSchemaOperation(IQuery parsedQuery)
    {
        return parsedQuery switch
        {
            CreateTableQuery => "create_table",
            AddColumnQuery => "add_column",
            PurgeColumnQuery => "purge_column",
            RenameColumnQuery => "rename_column",
            AlterColumnQuery => "alter_column",
            PurgeTableQuery => "purge_table",
            CreateIndexQuery => "create_index",
            PurgeIndexQuery => "purge_index",
            PurgeTtlQuery => "purge_ttl",
            _ => null,
        };
    }

    private static string? GetTableNameForNotify(IQuery query) => query switch
    {
        UpsertQuery q => q.Table,
        DeleteQuery q => q.Table,
        CreateTableQuery q => q.Table,
        PurgeTableQuery q => q.Table,
        AddColumnQuery q => q.Table,
        PurgeColumnQuery q => q.Table,
        RenameColumnQuery q => q.Table,
        AlterColumnQuery q => q.Table,
        CreateIndexQuery q => q.Table,
        PurgeIndexQuery q => q.Table,
        PurgeTtlQuery q => q.Table,
        _ => null,
    };

    private static string EscapeString(string value)
    {
        return value.Replace("'", "\\'");
    }

    // ── Index metrics persistence ──────────────────────────

    private void PersistIndexMetrics()
    {
        var systemDbPath = Path.Combine(_dataDirectory, "_system");
        var metricsTablePath = Path.Combine(systemDbPath, "index_metrics");

        if (!_tableCache.TryGetTable(metricsTablePath, out var table) || table is null)
            return;

        foreach (var (key, m) in _indexMetrics.GetAll())
        {
            var tableName = Path.GetFileName(key.TablePath);
            var compositeKey = $"{key.TablePath}|{key.Column}";
            var lastUsed = m.LastUsedAt is not null
                ? $", last_used_at: '{m.LastUsedAt.Value:yyyy-MM-dd HH:mm:ss}'"
                : "";
            var indexCreated = m.IndexCreatedAt is not null
                ? $", index_created_at: '{m.IndexCreatedAt.Value:yyyy-MM-dd HH:mm:ss}'"
                : "";

            var upsertQuery =
                $"upsert index_metrics {{key: '{EscapeString(compositeKey)}', table_name: '{EscapeString(tableName)}', column_name: '{EscapeString(key.Column)}', " +
                $"query_count: {m.QueryCount}, where_hit_count: {m.WhereHitCount}, read_count: {m.ReadCount}, write_count: {m.WriteCount}, " +
                $"scanned_total: {m.ScannedRowsTotal}, result_total: {m.ResultRowsTotal}, is_manual: {(m.IsManual ? "true" : "false")}" +
                $"{lastUsed}{indexCreated}}} on key";

            var parseResult = QueryParser.Parse(upsertQuery);
            if (parseResult.Success && parseResult.Query is UpsertQuery uq)
                UpsertExecutor.Execute(upsertQuery, table, uq, _settings.BulkLimit);
        }
    }

    private void LoadIndexMetrics()
    {
        var systemDbPath = Path.Combine(_dataDirectory, "_system");
        var metricsTablePath = Path.Combine(systemDbPath, "index_metrics");

        if (!_tableCache.TryGetTable(metricsTablePath, out var table) || table is null)
            return;

        var getQuery = "get index_metrics";
        var parseResult = QueryParser.Parse(getQuery);
        if (!parseResult.Success || parseResult.Query is not GetQuery gq)
            return;

        var result = GetExecutor.Execute(getQuery, table, gq, int.MaxValue, null);
        if (result.Data is null)
            return;

        foreach (var row in result.Data)
        {
            var key = row.TryGetValue("key", out var k) ? k?.ToString() : null;
            if (key is null) continue;

            var parts = key.Split('|', 2);
            if (parts.Length != 2) continue;

            var tablePath = parts[0];
            var column = parts[1];

            var m = _indexMetrics.GetOrCreate(tablePath, column);
            m.Load(
                queryCount: ToLong(row, "query_count"),
                whereHitCount: ToLong(row, "where_hit_count"),
                readCount: ToLong(row, "read_count"),
                writeCount: ToLong(row, "write_count"),
                scannedTotal: ToLong(row, "scanned_total"),
                resultTotal: ToLong(row, "result_total"),
                isManual: row.TryGetValue("is_manual", out var manual) && manual is true,
                lastUsedAt: ToDateTime(row, "last_used_at"),
                indexCreatedAt: ToDateTime(row, "index_created_at")
            );
        }
    }

    private static long ToLong(Dictionary<string, object?> row, string key)
    {
        if (!row.TryGetValue(key, out var val) || val is null)
            return 0;
        return val is long l ? l : Convert.ToInt64(val);
    }

    private static DateTime? ToDateTime(Dictionary<string, object?> row, string key)
    {
        if (!row.TryGetValue(key, out var val) || val is null)
            return null;
        if (val is DateTime dt)
            return dt;
        if (val is string s && DateTime.TryParse(s, out var parsed))
            return parsed;
        return null;
    }

    // ── WAL replay ──────────────────────────────────────────

    private void ReplayWal(string dbPath, string dbName)
    {
        var walPath = Path.Combine(dbPath, "_wal");
        if (!File.Exists(walPath))
            return;

        var wal = _walManager.GetOrOpen(dbPath);
        var entries = wal.ReadAll();

        if (entries.Count == 0)
            return;

        // Determine which group IDs are rolled back (negative groupId)
        // and which are incomplete (groupId > 0 but no corresponding entries
        // that would indicate a commit — incomplete groups are also skipped)
        var rolledBackGroups = new HashSet<long>();
        var groupEntries = new Dictionary<long, List<WalEntry>>();

        foreach (var entry in entries)
        {
            if (entry.GroupId < 0)
            {
                rolledBackGroups.Add(-entry.GroupId);
            }
            else if (entry.GroupId > 0)
            {
                if (!groupEntries.ContainsKey(entry.GroupId))
                    groupEntries[entry.GroupId] = [];
                groupEntries[entry.GroupId].Add(entry);
            }
        }

        foreach (var entry in entries)
        {
            // Skip rolled-back transaction entries
            if (entry.GroupId < 0)
                continue;
            if (entry.GroupId > 0 && rolledBackGroups.Contains(entry.GroupId))
                continue;

            var parseResult = QueryParser.Parse(entry.Query);
            if (!parseResult.Success || parseResult.Query is null)
                continue;

            var replayQuery = parseResult.Query;

            // Inject resolved ID for auto-ID upserts during replay
            if (entry.ResolvedId > 0 && replayQuery is UpsertQuery upsert
                && upsert.Records.Count == 1
                && !upsert.Records[0].Exists(f => f.Name == "_id"))
            {
                upsert.Records[0].Insert(0, new UpsertField
                {
                    Name = "_id",
                    Value = new UpsertValue
                    {
                        Kind = UpsertValueKind.Integer,
                        Raw = entry.ResolvedId.ToString(),
                    },
                });
            }

            Dispatch(entry.Query, dbName, dbPath, replayQuery);
        }

        // Flush MMFs to disk, then truncate WAL
        _tableCache.FlushTablesForDatabase(dbPath);
        wal.Truncate();
    }

    // ── Background cycles ────────────────────────────────────

    private async Task RunFlushCycle(TimeSpan interval)
    {
        if (interval == Timeout.InfiniteTimeSpan)
            return;

        using var timer = new PeriodicTimer(interval);

        try
        {
            while (await timer.WaitForNextTickAsync(_disposeCts.Token))
            {
                PostFlushAll();
            }
        }
        catch (OperationCanceledException)
        {
            // Expected on dispose
        }
    }

    private void PostFlushAll()
    {
        var tcs = new TaskCompletionSource<SproutResponse>(TaskCreationOptions.RunContinuationsAsynchronously);
        var action = new Action(tcs, () =>
        {
            FlushAll();
            return new SproutResponse { Operation = SproutOperation.Get }; // dummy
        });
        _writeChannel.Writer.TryWrite(action);
    }

    private void FlushAll()
    {
        EvaluateAutoIndexes();
        PersistIndexMetrics();
        _tableCache.FlushAll();
        _walManager.TruncateAll();
    }

    private void EvaluateAutoIndexes()
    {
        if (!_settings.AutoIndex.Enabled) return;

        var now = DateTime.UtcNow;

        foreach (var (key, metrics) in _indexMetrics.GetAll())
        {
            // Skip columns that don't have enough WHERE hits yet
            if (metrics.WhereHitCount < _settings.AutoIndex.MinimumQueryCount)
                continue;

            // Derive dbName and tableName from the tablePath
            // tablePath = {dataDir}/{db}/{table}
            var tableName = Path.GetFileName(key.TablePath);
            var dbPath = Path.GetDirectoryName(key.TablePath);
            if (dbPath is null) continue;
            var dbName = Path.GetFileName(dbPath);

            if (!_tableCache.TryGetTable(key.TablePath, out var table) || table is null)
                continue;

            if (!table.HasBTree(key.Column)
                && AutoIndexEvaluator.ShouldCreate(metrics, _settings.AutoIndex))
            {
                var createQuery = $"create index {tableName}.{key.Column}";
                var parseResult = QueryParser.Parse(createQuery);
                if (parseResult.Success && parseResult.Query is not null)
                {
                    Dispatch(createQuery, dbName, dbPath, parseResult.Query);
                    metrics.IndexCreatedAt = now;
                    metrics.IsManual = false; // Dispatch calls MarkManual — override back to auto

                    var reason = $"auto-index created: where_hits={metrics.WhereHitCount}, queries={metrics.QueryCount}, reads={metrics.ReadCount}, writes={metrics.WriteCount}";
                    LogAudit(dbName, $"{createQuery} -- {reason}", "auto_create_index");
                }
            }
            else if (table.HasBTree(key.Column)
                     && !metrics.IsManual
                     && AutoIndexEvaluator.ShouldRemove(metrics, _settings.AutoIndex, now))
            {
                var purgeQuery = $"purge index {tableName}.{key.Column}";
                var parseResult = QueryParser.Parse(purgeQuery);
                if (parseResult.Success && parseResult.Query is not null)
                {
                    Dispatch(purgeQuery, dbName, dbPath, parseResult.Query);
                    metrics.IndexCreatedAt = null;

                    var daysSinceUsed = metrics.LastUsedAt is not null
                        ? (int)(now - metrics.LastUsedAt.Value).TotalDays
                        : -1;
                    var reason = $"auto-index removed: unused for {daysSinceUsed} days (threshold={_settings.AutoIndex.UnusedRetentionDays})";
                    LogAudit(dbName, $"{purgeQuery} -- {reason}", "auto_purge_index");
                }
            }
        }
    }

    private async Task RunWalSyncCycle(TimeSpan interval)
    {
        if (interval == TimeSpan.Zero || interval == Timeout.InfiniteTimeSpan)
            return;

        using var timer = new PeriodicTimer(interval);

        try
        {
            while (await timer.WaitForNextTickAsync(_disposeCts.Token))
            {
                PostSyncAllWals();
            }
        }
        catch (OperationCanceledException)
        {
            // Expected on dispose
        }
    }

    private void PostSyncAllWals()
    {
        var tcs = new TaskCompletionSource<SproutResponse>(TaskCreationOptions.RunContinuationsAsynchronously);
        var action = new Action(tcs, () =>
        {
            _walManager.SyncAll();
            return new SproutResponse { Operation = SproutOperation.Get }; // dummy
        });
        _writeChannel.Writer.TryWrite(action);
    }

    // ── TTL cleanup ────────────────────────────────────────

    private int _ttlCleanupRoundRobinIndex;

    private async Task RunTtlCleanupCycle(TimeSpan interval)
    {
        if (interval == Timeout.InfiniteTimeSpan)
            return;

        using var timer = new PeriodicTimer(interval);

        try
        {
            while (await timer.WaitForNextTickAsync(_disposeCts.Token))
            {
                PostTtlCleanup();
            }
        }
        catch (OperationCanceledException)
        {
            // Expected on dispose
        }
    }

    private void PostTtlCleanup()
    {
        var tcs = new TaskCompletionSource<SproutResponse>(TaskCreationOptions.RunContinuationsAsynchronously);
        var action = new Action(tcs, () =>
        {
            ExecuteTtlCleanup();
            return new SproutResponse { Operation = SproutOperation.Get }; // dummy
        });
        _writeChannel.Writer.TryWrite(action);
    }

    private void ExecuteTtlCleanup()
    {
        // Collect all tables that have TTL enabled
        var ttlTables = new List<(string Path, Storage.TableHandle Table)>();
        foreach (var (path, table) in _tableCache.GetAllOpened())
        {
            if (table.HasTtl)
                ttlTables.Add((path, table));
        }

        if (ttlTables.Count == 0) return;

        // Round-robin: pick one table per pass
        var idx = _ttlCleanupRoundRobinIndex % ttlTables.Count;
        _ttlCleanupRoundRobinIndex = idx + 1;
        var (_, target) = ttlTables[idx];

        var ttl = target.Ttl;
        if (ttl is null) return;

        var nowMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
        var maxBatch = _settings.TtlCleanupBatchSize;

        // Collect expired rows
        var expired = new List<long>(); // places
        target.Index.ForEachUsed((id, place) =>
        {
            if (expired.Count >= maxBatch) return;

            var expiresAt = ttl.ReadExpiresAt(place);
            if (expiresAt > 0 && nowMs > expiresAt)
                expired.Add(place);
        });

        // Delete expired rows
        foreach (var place in expired)
        {
            // Remove from B-Trees
            foreach (var col in target.Schema.Columns)
            {
                if (target.HasBTree(col.Name))
                {
                    var colHandle = target.GetColumn(col.Name);
                    if (!colHandle.IsNullAtPlace(place))
                    {
                        var val = colHandle.ReadValue(place);
                        if (val is not null)
                        {
                            var encoded = colHandle.EncodeValueToBytes(val.ToString() ?? "");
                            target.GetBTree(col.Name).Remove(encoded, place);
                        }
                    }
                }
            }

            // Free slot
            target.Index.FreeSlot(place);

            // Clear TTL
            ttl.Clear(place);

            // Null out columns
            foreach (var col in target.Schema.Columns)
                target.GetColumn(col.Name).WriteNull(place);
        }
    }

    // ── WAL ID resolution ────────────────────────────────────

    private static bool IsMutatingQuery(IQuery query)
    {
        return query is CreateTableQuery or UpsertQuery or AddColumnQuery
            or PurgeColumnQuery or PurgeTableQuery or PurgeDatabaseQuery
            or RenameColumnQuery or AlterColumnQuery or DeleteQuery
            or CreateIndexQuery or PurgeIndexQuery or PurgeTtlQuery
            or ShrinkTableQuery or ShrinkDatabaseQuery;
    }

    /// <summary>
    /// For auto-ID upserts, reads the next ID that will be assigned.
    /// This ID is stored in the WAL entry so replay is idempotent.
    /// Returns 0 for non-upsert or explicit-ID upsert queries.
    /// </summary>
    private ulong ResolveIdForWal(IQuery query, string dbPath)
    {
        if (query is not UpsertQuery upsert)
            return 0;

        if (upsert.Records.Count != 1 || upsert.Records[0].Exists(f => f.Name == "_id"))
            return 0;

        var tablePath = Path.Combine(dbPath, upsert.Table);
        if (!_tableCache.TryGetTable(tablePath, out var table) || table is null)
            return 0;

        return table.Index.ReadNextId();
    }

    // ── Query dispatch ───────────────────────────────────────

    private SproutResponse ExecuteGet(string query, string dbName, string dbPath, GetQuery q)
    {
        if (!_tableCache.DatabaseExists(dbPath))
            return ResponseHelper.Error(query, ErrorCodes.UNKNOWN_DATABASE,
                $"database '{dbName}' does not exist");

        Func<string, TableHandle?>? tableResolver = q.Follow is not null
            ? name => ResolveTable(dbPath, name)
            : null;

        var tablePath = Path.Combine(dbPath, q.Table);

        var result = ExecuteWithTable(query, dbPath, q.Table,
            table => GetExecutor.Execute(query, table, q, _settings.DefaultPageSize, tableResolver));

        // Track index metrics for WHERE, ORDER BY, and FOLLOW columns
        if (result.Errors is null)
        {
            Interlocked.Increment(ref _totalReads);
            _indexMetrics.RecordQuery(tablePath);

            // Collect all index-relevant columns
            var indexColumns = WhereEngine.ExtractWhereColumns(q.Where);

            if (q.OrderBy is { Count: > 0 } orderCols)
            {
                foreach (var ob in orderCols)
                    indexColumns.Add(ob.Name);
            }

            foreach (var col in indexColumns)
            {
                _indexMetrics.RecordWhereUsage(tablePath, col);
                _indexMetrics.RecordRead(tablePath, col);
            }

            // Track FOLLOW target columns (FK join targets benefit hugely from indexes)
            if (q.Follow is { Count: > 0 } followClauses)
            {
                foreach (var fc in followClauses)
                {
                    var targetTablePath = Path.Combine(dbPath, fc.TargetTable);
                    _indexMetrics.RecordQuery(targetTablePath);
                    _indexMetrics.RecordWhereUsage(targetTablePath, fc.TargetColumn);
                    _indexMetrics.RecordRead(targetTablePath, fc.TargetColumn);
                }
            }

            // Record scan statistics for selectivity analysis
            if (result.Data is not null && _tableCache.TryGetTable(tablePath, out var tableForStats) && tableForStats is not null)
            {
                var scannedCount = (long)tableForStats.Index.ReadNextId();
                var resultCount = result.Data.Count;
                foreach (var col in indexColumns)
                {
                    _indexMetrics.RecordScanStats(tablePath, col, scannedCount, resultCount);
                }
            }
        }

        return result;
    }

    private SproutResponse ExecuteDescribe(string query, string dbName, string dbPath, DescribeQuery q)
    {
        if (!_tableCache.DatabaseExists(dbPath))
            return ResponseHelper.Error(query, ErrorCodes.UNKNOWN_DATABASE,
                $"database '{dbName}' does not exist");

        // describe (no table) → list all tables
        if (q.Table is null)
        {
            Interlocked.Increment(ref _totalReads);
            return DescribeExecutor.ExecuteAll(query, dbPath);
        }

        // describe <table> → show columns
        Interlocked.Increment(ref _totalReads);
        var describeTablePath = Path.Combine(dbPath, q.Table);
        Func<string, bool> isAutoIndex = col =>
        {
            var m = _indexMetrics.TryGet(describeTablePath, col);
            return m is not null && !m.IsManual;
        };
        var describeEffectiveChunkSize = ResolveChunkSize(0, dbPath);
        return ExecuteWithTable(query, dbPath, q.Table,
            table =>
            {
                var ecs = table.Schema.ChunkSize > 0 ? table.Schema.ChunkSize : describeEffectiveChunkSize;
                return DescribeExecutor.ExecuteTable(query, table, q.Table, ecs, isAutoIndex);
            });
    }

    private SproutResponse Dispatch(string query, string dbName, string dbPath, IQuery parsedQuery,
        Storage.TransactionJournal? journal = null)
    {
        return parsedQuery switch
        {
            CreateDatabaseQuery q => ExecuteCreateDatabase(query, dbName, dbPath, q),
            CreateTableQuery q => ExecuteCreateTable(query, dbName, dbPath, q),
            GetQuery q => ExecuteWithTable(query, dbPath, q.Table, table => GetExecutor.Execute(query, table, q, _settings.DefaultPageSize, q.Follow is not null ? name => ResolveTable(dbPath, name) : null)),
            UpsertQuery q => ExecuteWithTable(query, dbPath, q.Table, table => UpsertExecutor.Execute(query, table, q, _settings.BulkLimit, journal)),
            AddColumnQuery q => ExecuteWithTable(query, dbPath, q.Table, table => AddColumnExecutor.Execute(query, table, q)),
            PurgeColumnQuery q => ExecuteWithTable(query, dbPath, q.Table, table => PurgeColumnExecutor.Execute(query, table, q)),
            PurgeTableQuery q => ExecutePurgeTable(query, dbPath, q),
            PurgeDatabaseQuery => ExecutePurgeDatabase(query, dbName, dbPath),
            RenameColumnQuery q => ExecuteWithTable(query, dbPath, q.Table, table => RenameColumnExecutor.Execute(query, table, q)),
            AlterColumnQuery q => ExecuteWithTable(query, dbPath, q.Table, table => AlterColumnExecutor.Execute(query, table, q)),
            DeleteQuery q => ExecuteWithTable(query, dbPath, q.Table, table => DeleteExecutor.Execute(query, table, q, journal)),
            CreateIndexQuery q => ExecuteWithTable(query, dbPath, q.Table, table => CreateIndexExecutor.Execute(query, table, q)),
            PurgeIndexQuery q => ExecuteWithTable(query, dbPath, q.Table, table => PurgeIndexExecutor.Execute(query, table, q)),
            PurgeTtlQuery q => ExecuteWithTable(query, dbPath, q.Table, table => ExecutePurgeTtl(query, table, q)),
            ShrinkTableQuery q => ExecuteShrinkTable(query, dbPath, q),
            ShrinkDatabaseQuery q => ExecuteShrinkDatabase(query, dbName, dbPath, q),
            _ => ResponseHelper.Error(query, ErrorCodes.SYNTAX_ERROR, "operation not supported"),
        };
    }

    // ── Database-level execution ─────────────────────────────

    private SproutResponse ExecuteCreateDatabase(string query, string dbName, string dbPath, CreateDatabaseQuery q)
    {
        var result = CreateDatabaseExecutor.Execute(query, dbName, dbPath, q.ChunkSize);
        if (result.Errors is null)
            _tableCache.RegisterDatabase(dbPath);
        return result;
    }

    private SproutResponse ExecuteCreateTable(string query, string dbName, string dbPath, CreateTableQuery q)
    {
        if (!_tableCache.DatabaseExists(dbPath))
            return ResponseHelper.Error(query, ErrorCodes.UNKNOWN_DATABASE,
                $"database '{dbName}' does not exist");

        var effectiveChunkSize = ResolveChunkSize(q.ChunkSize, dbPath);
        return CreateTableExecutor.Execute(query, dbName, dbPath, q, effectiveChunkSize);
    }

    private int ResolveChunkSize(int tableChunkSize, string dbPath)
    {
        if (tableChunkSize > 0) return tableChunkSize;

        var metaPath = Path.Combine(dbPath, "_meta.bin");
        if (File.Exists(metaPath))
        {
            var (_, dbChunkSize) = MetaFile.Read(metaPath);
            if (dbChunkSize > 0) return dbChunkSize;
        }

        return _settings.ChunkSize;
    }

    private SproutResponse ExecuteBackup(string query, string dbName, string dbPath)
    {
        if (!_tableCache.DatabaseExists(dbPath))
            return ResponseHelper.Error(query, ErrorCodes.UNKNOWN_DATABASE,
                $"database '{dbName}' does not exist");

        // Flush all MMFs + WAL to ensure consistent on-disk state
        _tableCache.FlushTablesForDatabase(dbPath);
        var wal = _walManager.GetOrOpen(dbPath);
        wal.SyncToDisk();

        return BackupExecutor.Execute(query, dbName, dbPath);
    }

    private SproutResponse ExecuteRestore(string query, string dbName, string dbPath, RestoreQuery q)
    {
        // Evict any cached state for this database
        _tableCache.EvictTablesForDatabase(dbPath);
        _walManager.Evict(dbPath);
        _tableCache.UnregisterDatabase(dbPath);

        var result = RestoreExecutor.Execute(query, dbName, dbPath, q.FilePath);
        if (result.Errors is null)
        {
            // Re-register and open restored tables
            _tableCache.RegisterDatabase(dbPath);
            _tableCache.OpenTablesForDatabase(dbPath);
        }
        return result;
    }

    private SproutResponse ExecutePurgeDatabase(string query, string dbName, string dbPath)
    {
        _tableCache.EvictTablesForDatabase(dbPath);
        _walManager.Evict(dbPath);
        _tableCache.UnregisterDatabase(dbPath);

        return PurgeDatabaseExecutor.Execute(query, dbName, dbPath);
    }

    private SproutResponse ExecutePurgeTable(string query, string dbPath, PurgeTableQuery q)
    {
        var tablePath = Path.Combine(dbPath, q.Table);
        _tableCache.EvictTable(tablePath);

        return PurgeTableExecutor.Execute(query, q, dbPath);
    }

    private static SproutResponse ExecutePurgeTtl(string query, TableHandle table, PurgeTtlQuery q)
    {
        // Set table TTL to 0 (no TTL). Row-TTLs remain in _ttl file but table-level is cleared.
        table.Schema.TtlSeconds = 0;
        table.SaveSchema();

        return new SproutResponse
        {
            Operation = SproutOperation.PurgeTtl,
            Schema = new SchemaInfo { Table = q.Table },
        };
    }

    private SproutResponse ExecuteShrinkTable(string query, string dbPath, ShrinkTableQuery q)
    {
        var tablePath = Path.Combine(dbPath, q.Table);
        if (!_tableCache.TryGetTable(tablePath, out _))
        {
            if (!Directory.Exists(tablePath))
                return ResponseHelper.Error(query, ErrorCodes.UNKNOWN_TABLE,
                    $"table '{q.Table}' does not exist");
        }

        var table = _tableCache.GetOrOpen(tablePath);
        var effectiveChunkSize = q.ChunkSize > 0 ? q.ChunkSize : ResolveChunkSize(table.Schema.ChunkSize, dbPath);

        // Collect slot info while handle is still open
        var slotInfo = ShrinkTableExecutor.CollectSlotInfo(table);

        // Flush and evict (releases MMFs so files can be replaced)
        table.Flush();
        _tableCache.EvictTable(tablePath);

        return ShrinkTableExecutor.Execute(query, tablePath, q.Table, effectiveChunkSize, slotInfo);
    }

    private SproutResponse ExecuteShrinkDatabase(string query, string dbName, string dbPath, ShrinkDatabaseQuery q)
    {
        if (!_tableCache.DatabaseExists(dbPath))
            return ResponseHelper.Error(query, ErrorCodes.UNKNOWN_DATABASE,
                $"database '{dbName}' does not exist");

        // Flush all cached tables for this database
        _tableCache.FlushTablesForDatabase(dbPath);

        return ShrinkDatabaseExecutor.Execute(
            query, dbName, dbPath, q.ChunkSize, _settings.ChunkSize,
            tableName =>
            {
                var tp = Path.Combine(dbPath, tableName);
                return _tableCache.GetOrOpen(tp);
            },
            tablePath =>
            {
                if (_tableCache.TryGetTable(tablePath, out var t) && t is not null)
                    t.Flush();
                _tableCache.EvictTable(tablePath);
            });
    }

    // ── Table-scoped execution ──────────────────────────────

    private SproutResponse ExecuteWithTable(
        string query, string dbPath, string tableName,
        Func<TableHandle, SproutResponse> executor)
    {
        var tablePath = Path.Combine(dbPath, tableName);

        // Fast path: table already cached
        if (_tableCache.TryGetTable(tablePath, out var cached) && cached is not null)
            return executor(cached);

        // Cold path: check filesystem
        if (!_tableCache.DatabaseExists(dbPath))
            return ResponseHelper.Error(query, ErrorCodes.UNKNOWN_DATABASE,
                $"database '{Path.GetFileName(dbPath)}' does not exist");

        if (!Directory.Exists(tablePath))
            return ResponseHelper.Error(query, ErrorCodes.UNKNOWN_TABLE,
                $"table '{tableName}' does not exist");

        var table = _tableCache.GetOrOpen(tablePath);
        return executor(table);
    }

    /// <summary>
    /// Resolves a table by name within a database. Returns null if the table does not exist.
    /// </summary>
    private TableHandle? ResolveTable(string dbPath, string tableName)
    {
        var tablePath = Path.Combine(dbPath, tableName);
        if (_tableCache.TryGetTable(tablePath, out var cached) && cached is not null)
            return cached;
        if (!Directory.Exists(tablePath))
            return null;
        return _tableCache.GetOrOpen(tablePath);
    }

    // ── Validation ──────────────────────────────────────────

    internal static bool IsValidName(string name)
    {
        if (string.IsNullOrEmpty(name))
            return false;
        if (!char.IsAsciiLetter(name[0]) && name[0] != '_')
            return false;
        for (var i = 1; i < name.Length; i++)
        {
            if (!char.IsAsciiLetterOrDigit(name[i]) && name[i] != '_')
                return false;
        }
        return true;
    }

    private static string LowercaseName(string name)
    {
        return string.Create(name.Length, name, static (span, src) =>
        {
            src.AsSpan().CopyTo(span);
            for (var i = 0; i < span.Length; i++)
                span[i] = char.ToLowerInvariant(span[i]);
        });
    }

    // ── ISproutServer ─────────────────────────────────────

    public ISproutDatabase GetOrCreateDatabase(string name)
    {
        var dbName = LowercaseName(name);
        if (!IsValidName(dbName))
            throw new InvalidDatabaseNameException(name, nameof(name));

        var dbPath = Path.Combine(_dataDirectory, dbName);

        if (!_tableCache.DatabaseExists(dbPath))
        {
            // Use ExecuteInternal to allow creating _-prefixed databases (e.g. _system)
            ExecuteInternal("create database", dbName);
        }

        return new SproutDatabase(this, dbName);
    }

    public ISproutDatabase SelectDatabase(string name)
    {
        var dbName = LowercaseName(name);
        if (!IsValidName(dbName))
            throw new InvalidDatabaseNameException(name, nameof(name));

        var dbPath = Path.Combine(_dataDirectory, dbName);

        if (!_tableCache.DatabaseExists(dbPath))
            throw new InvalidOperationException($"database '{dbName}' does not exist");

        return new SproutDatabase(this, dbName);
    }

    public SproutMetrics GetMetrics()
    {
        var dbMetrics = new List<DatabaseMetrics>();
        long totalStorage = 0;
        int totalTableCount = 0;
        long totalRows = 0;

        if (Directory.Exists(_dataDirectory))
        {
            foreach (var dbDir in Directory.GetDirectories(_dataDirectory))
            {
                var dbName = Path.GetFileName(dbDir);
                var tableMetrics = new List<TableMetrics>();
                long dbStorage = 0;
                long dbRows = 0;

                foreach (var tableDir in Directory.GetDirectories(dbDir))
                {
                    var schemaPath = Path.Combine(tableDir, "_schema.bin");
                    if (!File.Exists(schemaPath))
                        continue;

                    var tableName = Path.GetFileName(tableDir);
                    long rowCount = 0;
                    long storageBytes = 0;
                    int columnCount = 0;
                    int indexCount = 0;
                    long createdTicks = 0;

                    if (_tableCache.TryGetTable(tableDir, out var table) && table is not null)
                    {
                        rowCount = table.Index.ActiveRowCount;
                        storageBytes = table.GetStorageSizeBytes();
                        columnCount = table.Schema.Columns.Count;
                        indexCount = table.IndexCount;
                        createdTicks = table.Schema.CreatedTicks;
                    }
                    else
                    {
                        // Table not opened — estimate storage from file sizes
                        foreach (var file in Directory.EnumerateFiles(tableDir))
                            storageBytes += new FileInfo(file).Length;
                    }

                    tableMetrics.Add(new TableMetrics
                    {
                        Name = tableName,
                        RowCount = rowCount,
                        StorageSizeBytes = storageBytes,
                        ColumnCount = columnCount,
                        IndexCount = indexCount,
                        CreatedAt = createdTicks > 0
                            ? new DateTime(createdTicks, DateTimeKind.Utc)
                            : DateTime.MinValue,
                    });

                    dbStorage += storageBytes;
                    dbRows += rowCount;
                }

                dbMetrics.Add(new DatabaseMetrics
                {
                    Name = dbName,
                    TableCount = tableMetrics.Count,
                    TotalRows = dbRows,
                    StorageSizeBytes = dbStorage,
                    Tables = tableMetrics,
                });

                totalStorage += dbStorage;
                totalTableCount += tableMetrics.Count;
                totalRows += dbRows;
            }
        }

        return new SproutMetrics
        {
            Uptime = DateTime.UtcNow - _startedAtUtc,
            TotalReads = Interlocked.Read(ref _totalReads),
            TotalWrites = Interlocked.Read(ref _totalWrites),
            StorageSizeBytes = totalStorage,
            WalSizeBytes = _walManager.GetTotalSizeBytes(),
            DatabaseCount = dbMetrics.Count,
            TableCount = totalTableCount,
            TotalRows = totalRows,
            Databases = dbMetrics,
        };
    }

    public IReadOnlyList<ISproutDatabase> GetDatabases()
    {
        var databases = new List<ISproutDatabase>();

        if (!Directory.Exists(_dataDirectory))
            return databases;

        foreach (var dbDir in Directory.GetDirectories(_dataDirectory))
        {
            var dbName = Path.GetFileName(dbDir);
            databases.Add(new SproutDatabase(this, dbName));
        }

        return databases;
    }

    public void Migrate(Assembly assembly, ISproutDatabase database)
    {
        MigrationRunner.Run(assembly, database);
    }


    public void Dispose()
    {
        if (_disposed) return;

        // 1. Signal background loops to stop immediately
        _disposed = true;
        _disposeCts.Cancel();

        // 2. Wait for timer loops to exit
        _flushTask.GetAwaiter().GetResult();
        _walSyncTask.GetAwaiter().GetResult();
        _ttlCleanupTask.GetAwaiter().GetResult();

        // 3. Complete the writer channel → writer loop drains and exits
        _writeChannel.Writer.Complete();
        _writerTask.GetAwaiter().GetResult();

        // 4. Final sync + flush on dispose thread (writer is done)
        _walManager.SyncAll();
        FlushAll();

        // 5. Dispose handles
        _scopes.Dispose();
        _changeNotifier.Dispose();
        _tableCache.Dispose();
        _walManager.Dispose();
        _disposeCts.Dispose();
    }

    // ── Write protection ──────────────────────────────────────

    private static bool IsProtectedName(string name) => name.Length > 0 && name[0] == '_';

    private static SproutResponse? CheckWriteProtection(string query, IQuery parsedQuery, string dbName)
    {
        // Database-level protection: _ prefixed databases are fully read-only
        if (IsProtectedName(dbName))
        {
            if (parsedQuery is CreateDatabaseQuery)
                return ProtectionError(query, $"cannot create database '{dbName}'");
            if (parsedQuery is PurgeDatabaseQuery)
                return ProtectionError(query, $"cannot purge database '{dbName}'");

            // All other writes into a protected database are blocked
            return ProtectionError(query, $"cannot write to database '{dbName}'");
        }

        // Table-level protection (within non-protected databases)
        return parsedQuery switch
        {
            CreateTableQuery q when IsProtectedName(q.Table)
                => ProtectionError(query, $"cannot create table '{q.Table}'"),

            UpsertQuery q when IsProtectedName(q.Table)
                => ProtectionError(query, $"cannot write to table '{q.Table}'"),

            DeleteQuery q when IsProtectedName(q.Table)
                => ProtectionError(query, $"cannot delete from table '{q.Table}'"),

            PurgeTableQuery q when IsProtectedName(q.Table)
                => ProtectionError(query, $"cannot purge table '{q.Table}'"),

            // Column-level protection
            AddColumnQuery q when IsProtectedName(q.Column.Name)
                => ProtectionError(query, $"cannot add column '{q.Column.Name}'"),

            PurgeColumnQuery q when IsProtectedName(q.Column)
                => ProtectionError(query, $"cannot purge column '{q.Column}'"),

            RenameColumnQuery q when IsProtectedName(q.OldColumn) || IsProtectedName(q.NewColumn)
                => ProtectionError(query, "cannot rename to or from columns starting with '_'"),

            AlterColumnQuery q when IsProtectedName(q.Column)
                => ProtectionError(query, $"cannot alter column '{q.Column}'"),

            CreateIndexQuery q when IsProtectedName(q.Column)
                => ProtectionError(query, $"cannot index column '{q.Column}'"),

            PurgeIndexQuery q when IsProtectedName(q.Column)
                => ProtectionError(query, $"cannot purge index on column '{q.Column}'"),

            _ => null,
        };
    }

    private static SproutResponse ProtectionError(string query, string detail)
        => ResponseHelper.Error(query, ErrorCodes.PROTECTED_NAME,
            $"{detail}: names starting with '_' are system-managed");

    // ── Action record ────────────────────────────────────────

    private sealed record Action(TaskCompletionSource<SproutResponse> Completion, Func<SproutResponse> Work);
}

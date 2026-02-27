using System.Threading.Channels;
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
public sealed class SproutEngine : IDisposable
{
    private readonly string _dataDirectory;
    private readonly SproutEngineSettings _settings;
    private readonly TableCache _tableCache = new();
    private readonly WalManager _walManager = new();

    // Writer channel
    private readonly Channel<Action> _writeChannel;
    private readonly Task _writerTask;

    // Background cycles
    private readonly Task _flushTask;
    private readonly Task _walSyncTask;
    private readonly TimeSpan _walSyncInterval;
    private volatile bool _disposed;

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
    {
        _settings = settings;
        _dataDirectory = Path.GetFullPath(settings.DataDirectory);
        Directory.CreateDirectory(_dataDirectory);

        _walSyncInterval = settings.WalSyncInterval;

        // ── Startup replay: discover all databases, replay WALs ──
        ReplayAllDatabases();

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
    }

    /// <summary>
    /// Executes a query against the specified database.
    /// </summary>
    public SproutResponse Execute(string query, string database)
    {
        var dbName = LowercaseName(database);

        if (!IsValidName(dbName))
            return ResponseHelper.Error(query, ErrorCodes.SYNTAX_ERROR,
                $"invalid database name '{database}'");

        var dbPath = Path.Combine(_dataDirectory, dbName);

        // Parse on caller thread (stateless)
        var parseResult = QueryParser.Parse(query);
        if (!parseResult.Success)
            return ResponseHelper.ParseError(parseResult);

        var parsedQuery = parseResult.Query;
        if (parsedQuery is null)
            return ResponseHelper.ParseError(parseResult);

        // Reads bypass the channel → run directly on caller thread
        if (parsedQuery is GetQuery getQuery)
            return ExecuteGet(query, dbName, dbPath, getQuery);

        if (parsedQuery is DescribeQuery describeQuery)
            return ExecuteDescribe(query, dbName, dbPath, describeQuery);

        // All mutations go through the writer channel
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
        if (parsedQuery is CreateDatabaseQuery)
            return ExecuteCreateDatabase(query, dbName, dbPath);

        if (parsedQuery is PurgeDatabaseQuery)
            return ExecutePurgeDatabase(query, dbName, dbPath);

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

        return Dispatch(query, dbName, dbPath, parsedQuery);
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

    private void ReplayWal(string dbPath, string dbName)
    {
        var walPath = Path.Combine(dbPath, "_wal");
        if (!File.Exists(walPath))
            return;

        var wal = _walManager.GetOrOpen(dbPath);
        var entries = wal.ReadAll();

        if (entries.Count == 0)
            return;

        foreach (var entry in entries)
        {
            var parseResult = QueryParser.Parse(entry.Query);
            if (!parseResult.Success || parseResult.Query is null)
                continue;

            var replayQuery = parseResult.Query;

            // Inject resolved ID for auto-ID upserts during replay
            if (entry.ResolvedId > 0 && replayQuery is UpsertQuery upsert
                && upsert.Records.Count == 1
                && !upsert.Records[0].Exists(f => f.Name == "id"))
            {
                upsert.Records[0].Insert(0, new UpsertField
                {
                    Name = "id",
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

        while (!_disposed)
        {
            if (!await timer.WaitForNextTickAsync())
                break;

            if (_disposed)
                break;

            PostFlushAll();
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
        _tableCache.FlushAll();
        _walManager.TruncateAll();
    }

    private async Task RunWalSyncCycle(TimeSpan interval)
    {
        if (interval == TimeSpan.Zero || interval == Timeout.InfiniteTimeSpan)
            return;

        using var timer = new PeriodicTimer(interval);

        while (!_disposed)
        {
            if (!await timer.WaitForNextTickAsync())
                break;

            if (_disposed)
                break;

            PostSyncAllWals();
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

    // ── WAL ID resolution ────────────────────────────────────

    private static bool IsMutatingQuery(IQuery query)
    {
        return query is CreateTableQuery or UpsertQuery or AddColumnQuery
            or PurgeColumnQuery or PurgeTableQuery or PurgeDatabaseQuery
            or RenameColumnQuery or AlterColumnQuery;
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

        if (upsert.Records.Count != 1 || upsert.Records[0].Exists(f => f.Name == "id"))
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

        return ExecuteWithTable(query, dbPath, q.Table,
            table => GetExecutor.Execute(query, table, q));
    }

    private SproutResponse ExecuteDescribe(string query, string dbName, string dbPath, DescribeQuery q)
    {
        if (!_tableCache.DatabaseExists(dbPath))
            return ResponseHelper.Error(query, ErrorCodes.UNKNOWN_DATABASE,
                $"database '{dbName}' does not exist");

        // describe (no table) → list all tables
        if (q.Table is null)
            return DescribeExecutor.ExecuteAll(query, dbPath);

        // describe <table> → show columns
        return ExecuteWithTable(query, dbPath, q.Table,
            table => DescribeExecutor.ExecuteTable(query, table, q.Table));
    }

    private SproutResponse Dispatch(string query, string dbName, string dbPath, IQuery parsedQuery)
    {
        return parsedQuery switch
        {
            CreateDatabaseQuery => ExecuteCreateDatabase(query, dbName, dbPath),
            CreateTableQuery q => ExecuteCreateTable(query, dbName, dbPath, q),
            GetQuery q => ExecuteWithTable(query, dbPath, q.Table, table => GetExecutor.Execute(query, table, q)),
            UpsertQuery q => ExecuteWithTable(query, dbPath, q.Table, table => UpsertExecutor.Execute(query, table, q, _settings.BulkLimit)),
            AddColumnQuery q => ExecuteWithTable(query, dbPath, q.Table, table => AddColumnExecutor.Execute(query, table, q)),
            PurgeColumnQuery q => ExecuteWithTable(query, dbPath, q.Table, table => PurgeColumnExecutor.Execute(query, table, q)),
            PurgeTableQuery q => ExecutePurgeTable(query, dbPath, q),
            PurgeDatabaseQuery => ExecutePurgeDatabase(query, dbName, dbPath),
            RenameColumnQuery q => ExecuteWithTable(query, dbPath, q.Table, table => RenameColumnExecutor.Execute(query, table, q)),
            AlterColumnQuery q => ExecuteWithTable(query, dbPath, q.Table, table => AlterColumnExecutor.Execute(query, table, q)),
            _ => ResponseHelper.Error(query, ErrorCodes.SYNTAX_ERROR, "operation not supported"),
        };
    }

    // ── Database-level execution ─────────────────────────────

    private SproutResponse ExecuteCreateDatabase(string query, string dbName, string dbPath)
    {
        var result = CreateDatabaseExecutor.Execute(query, dbName, dbPath);
        if (result.Errors is null)
            _tableCache.RegisterDatabase(dbPath);
        return result;
    }

    private SproutResponse ExecuteCreateTable(string query, string dbName, string dbPath, CreateTableQuery q)
    {
        if (!_tableCache.DatabaseExists(dbPath))
            return ResponseHelper.Error(query, ErrorCodes.UNKNOWN_DATABASE,
                $"database '{dbName}' does not exist");

        return CreateTableExecutor.Execute(query, dbName, dbPath, q);
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

    // ── Validation ──────────────────────────────────────────

    internal static bool IsValidName(string name)
    {
        if (string.IsNullOrEmpty(name))
            return false;
        if (!char.IsAsciiLetter(name[0]))
            return false;
        for (var i = 1; i < name.Length; i++)
        {
            if (!char.IsAsciiLetterOrDigit(name[i]))
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

    public void Dispose()
    {
        // 1. Signal background loops to stop
        _disposed = true;

        // 2. Wait for timer loops to exit
        _flushTask.GetAwaiter().GetResult();
        _walSyncTask.GetAwaiter().GetResult();

        // 3. Complete the writer channel → writer loop drains and exits
        _writeChannel.Writer.Complete();
        _writerTask.GetAwaiter().GetResult();

        // 4. Final sync + flush on dispose thread (writer is done)
        _walManager.SyncAll();
        FlushAll();

        // 5. Dispose handles
        _tableCache.Dispose();
        _walManager.Dispose();
    }

    // ── Action record ────────────────────────────────────────

    private sealed record Action(TaskCompletionSource<SproutResponse> Completion, Func<SproutResponse> Work);
}

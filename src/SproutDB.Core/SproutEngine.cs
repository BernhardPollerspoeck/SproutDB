using System.Collections.Concurrent;
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
    private readonly ConcurrentDictionary<string, Lazy<TableHandle>> _tables = new();
    private readonly Dictionary<string, WalFile> _wals = [];
    private readonly ConcurrentDictionary<string, byte> _knownDatabases = new();

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

        // All other mutations require the database to exist
        if (!DatabaseExists(dbPath))
            return ResponseHelper.Error(query, ErrorCodes.UNKNOWN_DATABASE,
                $"database '{dbName}' does not exist");

        // WAL append before mutation
        ulong resolvedId = ResolveIdForWal(parsedQuery, dbPath);
        var wal = GetOrOpenWal(dbPath);
        wal.Append(query, resolvedId);

        // Immediate fsync if group commit is disabled
        if (_walSyncInterval == TimeSpan.Zero)
            wal.SyncToDisk();

        return DispatchMutation(query, dbName, dbPath, parsedQuery);
    }

    // ── Startup replay ──────────────────────────────────────

    private void ReplayAllDatabases()
    {
        if (!Directory.Exists(_dataDirectory))
            return;

        foreach (var dbDir in Directory.GetDirectories(_dataDirectory))
        {
            var dbName = Path.GetFileName(dbDir);
            _knownDatabases[dbDir] = 0;

            // Open all existing tables for this database
            OpenTablesForDatabase(dbDir);

            // Replay WAL if present
            ReplayWal(dbDir, dbName);
        }
    }

    private void OpenTablesForDatabase(string dbPath)
    {
        foreach (var tableDir in Directory.GetDirectories(dbPath))
        {
            var schemaPath = Path.Combine(tableDir, "_schema.bin");
            if (!File.Exists(schemaPath))
                continue;

            GetOrOpenTable(tableDir);
        }
    }

    private void ReplayWal(string dbPath, string dbName)
    {
        var walPath = Path.Combine(dbPath, "_wal");
        if (!File.Exists(walPath))
            return;

        var wal = GetOrOpenWal(dbPath);
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

            DispatchAll(entry.Query, dbName, dbPath, replayQuery);
        }

        // Flush MMFs to disk, then truncate WAL
        FlushTablesForDatabase(dbPath);
        wal.Truncate();
    }

    // ── Flush cycle ──────────────────────────────────────────

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
        // Fire-and-forget: don't block timer thread
    }

    private void FlushAll()
    {
        // Flush all open table MMFs to disk
        foreach (var lazy in _tables.Values)
        {
            if (lazy.IsValueCreated)
                lazy.Value.Flush();
        }

        // Truncate all WALs (data is now durable on disk)
        foreach (var wal in _wals.Values)
        {
            if (!wal.IsEmpty)
                wal.Truncate();
        }
    }

    // ── WAL group commit ────────────────────────────────────

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
            SyncAllWals();
            return new SproutResponse { Operation = SproutOperation.Get }; // dummy
        });
        _writeChannel.Writer.TryWrite(action);
        // Fire-and-forget
    }

    private void SyncAllWals()
    {
        foreach (var wal in _wals.Values)
            wal.SyncToDisk();
    }

    // ── WAL ID resolution ────────────────────────────────────

    private static bool IsMutatingQuery(IQuery query)
    {
        return query is CreateTableQuery or UpsertQuery or AddColumnQuery;
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
        if (!_tables.TryGetValue(tablePath, out var lazy) || !lazy.IsValueCreated)
            return 0;

        return lazy.Value.Index.ReadNextId();
    }

    // ── Query dispatch ───────────────────────────────────────

    private SproutResponse ExecuteGet(string query, string dbName, string dbPath, GetQuery q)
    {
        if (!DatabaseExists(dbPath))
            return ResponseHelper.Error(query, ErrorCodes.UNKNOWN_DATABASE,
                $"database '{dbName}' does not exist");

        return ExecuteWithTable(query, dbPath, q.Table,
            table => GetExecutor.Execute(query, table, q));
    }

    private SproutResponse DispatchMutation(string query, string dbName, string dbPath, IQuery parsedQuery)
    {
        return parsedQuery switch
        {
            CreateTableQuery q => ExecuteCreateTable(query, dbName, dbPath, q),
            UpsertQuery q => ExecuteWithTable(query, dbPath, q.Table, table => UpsertExecutor.Execute(query, table, q, _settings.BulkLimit)),
            AddColumnQuery q => ExecuteWithTable(query, dbPath, q.Table, table => AddColumnExecutor.Execute(query, table, q)),
            _ => ResponseHelper.Error(query, ErrorCodes.SYNTAX_ERROR, "operation not supported"),
        };
    }

    /// <summary>
    /// Full dispatch used during replay (handles all query types including CreateDatabase).
    /// </summary>
    private SproutResponse DispatchAll(string query, string dbName, string dbPath, IQuery parsedQuery)
    {
        return parsedQuery switch
        {
            CreateDatabaseQuery => ExecuteCreateDatabase(query, dbName, dbPath),
            CreateTableQuery q => ExecuteCreateTable(query, dbName, dbPath, q),
            GetQuery q => ExecuteWithTable(query, dbPath, q.Table, table => GetExecutor.Execute(query, table, q)),
            UpsertQuery q => ExecuteWithTable(query, dbPath, q.Table, table => UpsertExecutor.Execute(query, table, q, _settings.BulkLimit)),
            AddColumnQuery q => ExecuteWithTable(query, dbPath, q.Table, table => AddColumnExecutor.Execute(query, table, q)),
            _ => ResponseHelper.Error(query, ErrorCodes.SYNTAX_ERROR, "operation not supported"),
        };
    }

    // ── Database-level execution ─────────────────────────────

    private SproutResponse ExecuteCreateDatabase(string query, string dbName, string dbPath)
    {
        var result = CreateDatabaseExecutor.Execute(query, dbName, dbPath);
        if (result.Errors is null)
            _knownDatabases[dbPath] = 0;
        return result;
    }

    private SproutResponse ExecuteCreateTable(string query, string dbName, string dbPath, CreateTableQuery q)
    {
        if (!DatabaseExists(dbPath))
            return ResponseHelper.Error(query, ErrorCodes.UNKNOWN_DATABASE,
                $"database '{dbName}' does not exist");

        return CreateTableExecutor.Execute(query, dbName, dbPath, q);
    }

    // ── Table-scoped execution ──────────────────────────────

    private SproutResponse ExecuteWithTable(
        string query, string dbPath, string tableName,
        Func<TableHandle, SproutResponse> executor)
    {
        var tablePath = Path.Combine(dbPath, tableName);

        // Fast path: table already cached
        if (_tables.TryGetValue(tablePath, out var lazy))
            return executor(lazy.Value);

        // Cold path: check filesystem
        if (!DatabaseExists(dbPath))
            return ResponseHelper.Error(query, ErrorCodes.UNKNOWN_DATABASE,
                $"database '{Path.GetFileName(dbPath)}' does not exist");

        if (!Directory.Exists(tablePath))
            return ResponseHelper.Error(query, ErrorCodes.UNKNOWN_TABLE,
                $"table '{tableName}' does not exist");

        var table = GetOrOpenTable(tablePath);
        return executor(table);
    }

    // ── Handle caches ────────────────────────────────────────

    private TableHandle GetOrOpenTable(string tablePath)
    {
        var lazy = _tables.GetOrAdd(tablePath,
            static path => new Lazy<TableHandle>(() => TableHandle.Open(path)));
        return lazy.Value;
    }

    private WalFile GetOrOpenWal(string dbPath)
    {
        if (!_wals.TryGetValue(dbPath, out var wal))
        {
            var walPath = Path.Combine(dbPath, "_wal");
            wal = new WalFile(walPath);
            _wals[dbPath] = wal;
        }
        return wal;
    }

    // ── Existence checks ────────────────────────────────────

    private bool DatabaseExists(string dbPath)
    {
        if (_knownDatabases.ContainsKey(dbPath))
            return true;

        if (!Directory.Exists(dbPath))
            return false;

        _knownDatabases[dbPath] = 0;
        return true;
    }

    // ── Flush helpers ────────────────────────────────────────

    private void FlushTablesForDatabase(string dbPath)
    {
        foreach (var (path, lazy) in _tables)
        {
            if (lazy.IsValueCreated && path.StartsWith(dbPath, StringComparison.Ordinal))
                lazy.Value.Flush();
        }
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
        SyncAllWals();
        FlushAll();

        // 5. Dispose handles
        foreach (var lazy in _tables.Values)
        {
            if (lazy.IsValueCreated)
                lazy.Value.Dispose();
        }
        _tables.Clear();

        foreach (var wal in _wals.Values)
            wal.Dispose();
        _wals.Clear();
    }

    // ── Action record ────────────────────────────────────────

    private sealed record Action(TaskCompletionSource<SproutResponse> Completion, Func<SproutResponse> Work);
}

using SproutDB.Core.Execution;
using SproutDB.Core.Parsing;
using SproutDB.Core.Storage;

namespace SproutDB.Core;

/// <summary>
/// Main entry point for the SproutDB engine.
/// Parses queries and dispatches execution to specialized executors.
/// Write path: WAL append → MMF update → Response.
/// Background: WAL group commit (fsync), flush cycle (MMF flush + WAL truncate).
/// </summary>
public sealed class SproutEngine : IDisposable
{
    private readonly string _dataDirectory;
    private readonly Dictionary<string, TableHandle> _tables = [];
    private readonly Dictionary<string, WalFile> _wals = [];
    private readonly HashSet<string> _knownDatabases = [];
    private readonly HashSet<string> _replayedDatabases = [];

    // Flush cycle
    private readonly CancellationTokenSource _flushCts = new();
    private readonly Task _flushTask;

    // WAL group commit
    private readonly CancellationTokenSource _walSyncCts = new();
    private readonly Task _walSyncTask;
    private readonly TimeSpan _walSyncInterval;

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
        _dataDirectory = Path.GetFullPath(settings.DataDirectory);
        Directory.CreateDirectory(_dataDirectory);

        _walSyncInterval = settings.WalSyncInterval;
        _flushTask = RunFlushCycle(settings.FlushInterval, _flushCts.Token);
        _walSyncTask = RunWalSyncCycle(_walSyncInterval, _walSyncCts.Token);
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

        // Replay WAL on first access to this database
        ReplayWalIfNeeded(dbPath, dbName);

        var parseResult = QueryParser.Parse(query);
        if (!parseResult.Success)
            return ResponseHelper.ParseError(parseResult);

        var parsedQuery = parseResult.Query;
        if (parsedQuery is null)
            return ResponseHelper.ParseError(parseResult);

        // WAL write before mutating operations (except create database)
        if (IsMutatingQuery(parsedQuery))
        {
            if (!DatabaseExists(dbPath))
                return ResponseHelper.Error(query, ErrorCodes.UNKNOWN_DATABASE,
                    $"database '{dbName}' does not exist");

            ulong resolvedId = ResolveIdForWal(parsedQuery, dbPath);
            var wal = GetOrOpenWal(dbPath);
            wal.Append(query, resolvedId);

            // Immediate fsync if group commit is disabled
            if (_walSyncInterval == TimeSpan.Zero)
                wal.SyncToDisk();
        }

        return DispatchQuery(query, dbName, dbPath, parsedQuery);
    }

    // ── Flush cycle ──────────────────────────────────────────

    private async Task RunFlushCycle(TimeSpan interval, CancellationToken ct)
    {
        if (interval == Timeout.InfiniteTimeSpan)
            return;

        using var timer = new PeriodicTimer(interval);

        try
        {
            while (await timer.WaitForNextTickAsync(ct))
            {
                FlushAll();
            }
        }
        catch (OperationCanceledException)
        {
            // Expected on dispose
        }
    }

    private void FlushAll()
    {
        // Flush all open table MMFs to disk
        foreach (var table in _tables.Values)
            table.Flush();

        // Truncate all WALs (data is now durable on disk)
        foreach (var wal in _wals.Values)
        {
            if (!wal.IsEmpty)
                wal.Truncate();
        }
    }

    // ── WAL group commit ────────────────────────────────────

    private async Task RunWalSyncCycle(TimeSpan interval, CancellationToken ct)
    {
        if (interval == TimeSpan.Zero || interval == Timeout.InfiniteTimeSpan)
            return;

        using var timer = new PeriodicTimer(interval);

        try
        {
            while (await timer.WaitForNextTickAsync(ct))
            {
                SyncAllWals();
            }
        }
        catch (OperationCanceledException)
        {
            // Expected on dispose
        }
    }

    private void SyncAllWals()
    {
        foreach (var wal in _wals.Values)
            wal.SyncToDisk();
    }

    // ── WAL replay ───────────────────────────────────────────

    private void ReplayWalIfNeeded(string dbPath, string dbName)
    {
        if (_replayedDatabases.Contains(dbPath)) return;
        _replayedDatabases.Add(dbPath);

        if (!Directory.Exists(dbPath)) return;
        _knownDatabases.Add(dbPath);

        var walPath = Path.Combine(dbPath, "_wal");
        if (!File.Exists(walPath)) return;

        var wal = GetOrOpenWal(dbPath);
        var entries = wal.ReadAll();

        if (entries.Count == 0) return;

        foreach (var entry in entries)
        {
            var parseResult = QueryParser.Parse(entry.Query);
            if (!parseResult.Success || parseResult.Query is null)
                continue; // skip corrupted/invalid entries

            var replayQuery = parseResult.Query;

            // Inject resolved ID for auto-ID upserts during replay
            if (entry.ResolvedId > 0 && replayQuery is UpsertQuery upsert
                && !upsert.Fields.Exists(f => f.Name == "id"))
            {
                upsert.Fields.Insert(0, new UpsertField
                {
                    Name = "id",
                    Value = new UpsertValue
                    {
                        Kind = UpsertValueKind.Integer,
                        Raw = entry.ResolvedId.ToString(),
                    },
                });
            }

            DispatchQuery(entry.Query, dbName, dbPath, replayQuery);
        }

        // Flush MMFs to disk, then truncate WAL
        FlushTablesForDatabase(dbPath);
        wal.Truncate();
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

        if (upsert.Fields.Exists(f => f.Name == "id"))
            return 0;

        var tablePath = Path.Combine(dbPath, upsert.Table);
        if (!_tables.TryGetValue(tablePath, out var table))
            return 0; // table not cached = doesn't exist or first access, will fail at dispatch

        return table.Index.ReadNextId();
    }

    // ── Query dispatch ───────────────────────────────────────

    private SproutResponse DispatchQuery(string query, string dbName, string dbPath, IQuery parsedQuery)
    {
        return parsedQuery switch
        {
            CreateDatabaseQuery => ExecuteCreateDatabase(query, dbName, dbPath),
            CreateTableQuery q => ExecuteCreateTable(query, dbName, dbPath, q),
            GetQuery q => ExecuteWithTable(query, dbPath, q.Table, table => GetExecutor.Execute(query, table, q)),
            UpsertQuery q => ExecuteWithTable(query, dbPath, q.Table, table => UpsertExecutor.Execute(query, table, q)),
            AddColumnQuery q => ExecuteWithTable(query, dbPath, q.Table, table => AddColumnExecutor.Execute(query, table, q)),
            _ => ResponseHelper.Error(query, ErrorCodes.SYNTAX_ERROR, "operation not supported"),
        };
    }

    // ── Database-level execution ─────────────────────────────

    private SproutResponse ExecuteCreateDatabase(string query, string dbName, string dbPath)
    {
        var result = CreateDatabaseExecutor.Execute(query, dbName, dbPath);
        if (result.Errors is null)
            _knownDatabases.Add(dbPath);
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

        // Fast path: table already cached → db and table both exist
        if (_tables.TryGetValue(tablePath, out var table))
            return executor(table);

        // Cold path: check filesystem
        if (!DatabaseExists(dbPath))
            return ResponseHelper.Error(query, ErrorCodes.UNKNOWN_DATABASE,
                $"database '{Path.GetFileName(dbPath)}' does not exist");

        if (!Directory.Exists(tablePath))
            return ResponseHelper.Error(query, ErrorCodes.UNKNOWN_TABLE,
                $"table '{tableName}' does not exist");

        table = GetOrOpenTable(tablePath);
        return executor(table);
    }

    // ── Handle caches ────────────────────────────────────────

    private TableHandle GetOrOpenTable(string tablePath)
    {
        if (!_tables.TryGetValue(tablePath, out var handle))
        {
            handle = TableHandle.Open(tablePath);
            _tables[tablePath] = handle;
        }
        return handle;
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
        if (_knownDatabases.Contains(dbPath))
            return true;

        if (!Directory.Exists(dbPath))
            return false;

        _knownDatabases.Add(dbPath);
        return true;
    }

    // ── Flush helpers ────────────────────────────────────────

    private void FlushTablesForDatabase(string dbPath)
    {
        foreach (var (path, handle) in _tables)
        {
            if (path.StartsWith(dbPath, StringComparison.Ordinal))
                handle.Flush();
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
        // Stop WAL sync cycle
        _walSyncCts.Cancel();
        _walSyncTask.GetAwaiter().GetResult();
        _walSyncCts.Dispose();

        // Final WAL sync: ensure all buffered WAL data is durable
        SyncAllWals();

        // Stop flush cycle
        _flushCts.Cancel();
        _flushTask.GetAwaiter().GetResult();
        _flushCts.Dispose();

        // Final flush: ensure all data is on disk
        FlushAll();

        foreach (var table in _tables.Values)
            table.Dispose();
        _tables.Clear();

        foreach (var wal in _wals.Values)
            wal.Dispose();
        _wals.Clear();
    }
}

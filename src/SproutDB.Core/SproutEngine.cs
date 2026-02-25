using System.Text;
using SproutDB.Core.Execution;
using SproutDB.Core.Parsing;
using SproutDB.Core.Storage;

namespace SproutDB.Core;

/// <summary>
/// Main entry point for the SproutDB engine.
/// Parses queries and dispatches execution to specialized executors.
/// Write path: WAL append + fsync → MMF update → Response.
/// Background flush cycle: MMF flush → WAL truncate.
/// </summary>
public sealed class SproutEngine : IDisposable
{
    private readonly string _dataDirectory;
    private readonly Dictionary<string, TableHandle> _tables = [];
    private readonly Dictionary<string, WalFile> _wals = [];
    private readonly HashSet<string> _replayedDatabases = [];

    // Flush cycle
    private readonly CancellationTokenSource _flushCts = new();
    private readonly Task _flushTask;

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

        _flushTask = RunFlushCycle(settings.FlushInterval, _flushCts.Token);
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
            if (!Directory.Exists(dbPath))
                return ResponseHelper.Error(query, ErrorCodes.UNKNOWN_DATABASE,
                    $"database '{dbName}' does not exist");

            var walQuery = ResolveWalQuery(query, parsedQuery, dbPath);
            var wal = GetOrOpenWal(dbPath);
            wal.Append(walQuery);
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

    // ── WAL replay ───────────────────────────────────────────

    private void ReplayWalIfNeeded(string dbPath, string dbName)
    {
        if (_replayedDatabases.Contains(dbPath)) return;
        _replayedDatabases.Add(dbPath);

        if (!Directory.Exists(dbPath)) return;

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

            DispatchQuery(entry.Query, dbName, dbPath, parseResult.Query);
        }

        // Flush MMFs to disk, then truncate WAL
        FlushTablesForDatabase(dbPath);
        wal.Truncate();
    }

    // ── WAL query resolution ─────────────────────────────────

    private static bool IsMutatingQuery(IQuery query)
    {
        return query is CreateTableQuery or UpsertQuery or AddColumnQuery;
    }

    private string ResolveWalQuery(string originalQuery, IQuery query, string dbPath)
    {
        // Only upsert without explicit ID needs modification for idempotent replay
        if (query is not UpsertQuery upsert)
            return originalQuery;

        if (upsert.Fields.Exists(f => f.Name == "id"))
            return originalQuery;

        var tablePath = Path.Combine(dbPath, upsert.Table);
        if (!Directory.Exists(tablePath))
            return originalQuery; // will fail at execution

        var table = GetOrOpenTable(tablePath);
        var nextId = table.Index.ReadNextId();

        return RebuildUpsertWithId(upsert, nextId);
    }

    private static string RebuildUpsertWithId(UpsertQuery q, ulong id)
    {
        var sb = new StringBuilder();
        sb.Append("upsert ");
        sb.Append(q.Table);
        sb.Append(" {id: ");
        sb.Append(id);

        foreach (var field in q.Fields)
        {
            sb.Append(", ");
            sb.Append(field.Name);
            sb.Append(": ");
            AppendFieldValue(sb, field.Value);
        }

        sb.Append('}');
        return sb.ToString();
    }

    private static void AppendFieldValue(StringBuilder sb, UpsertValue value)
    {
        switch (value.Kind)
        {
            case UpsertValueKind.Null:
                sb.Append("null");
                break;
            case UpsertValueKind.Boolean:
            case UpsertValueKind.Integer:
            case UpsertValueKind.Float:
                sb.Append(value.Raw);
                break;
            case UpsertValueKind.String:
                sb.Append('\'');
                sb.Append(value.Raw);
                sb.Append('\'');
                break;
        }
    }

    // ── Query dispatch ───────────────────────────────────────

    private SproutResponse DispatchQuery(string query, string dbName, string dbPath, IQuery parsedQuery)
    {
        return parsedQuery switch
        {
            CreateDatabaseQuery => CreateDatabaseExecutor.Execute(query, dbName, dbPath),
            CreateTableQuery q => CreateTableExecutor.Execute(query, dbName, dbPath, q),
            GetQuery q => ExecuteWithTable(query, dbPath, q.Table, table => GetExecutor.Execute(query, table, q)),
            UpsertQuery q => ExecuteWithTable(query, dbPath, q.Table, table => UpsertExecutor.Execute(query, table, q)),
            AddColumnQuery q => ExecuteWithTable(query, dbPath, q.Table, table => AddColumnExecutor.Execute(query, table, q)),
            _ => ResponseHelper.Error(query, ErrorCodes.SYNTAX_ERROR, "operation not supported"),
        };
    }

    // ── Table-scoped execution ──────────────────────────────

    private SproutResponse ExecuteWithTable(
        string query, string dbPath, string tableName,
        Func<TableHandle, SproutResponse> executor)
    {
        if (!Directory.Exists(dbPath))
            return ResponseHelper.Error(query, ErrorCodes.UNKNOWN_DATABASE,
                $"database '{Path.GetFileName(dbPath)}' does not exist");

        var tablePath = Path.Combine(dbPath, tableName);
        if (!Directory.Exists(tablePath))
            return ResponseHelper.Error(query, ErrorCodes.UNKNOWN_TABLE,
                $"table '{tableName}' does not exist");

        var table = GetOrOpenTable(tablePath);
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

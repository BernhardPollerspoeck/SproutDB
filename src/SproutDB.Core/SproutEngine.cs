using SproutDB.Core.Execution;
using SproutDB.Core.Parsing;
using SproutDB.Core.Storage;

namespace SproutDB.Core;

/// <summary>
/// Main entry point for the SproutDB engine.
/// Parses queries and dispatches execution to specialized executors.
/// </summary>
public sealed class SproutEngine : IDisposable
{
    private readonly string _dataDirectory;
    private readonly Dictionary<string, TableHandle> _tables = [];

    public SproutEngine(string dataDirectory)
    {
        _dataDirectory = Path.GetFullPath(dataDirectory);
        Directory.CreateDirectory(_dataDirectory);
    }

    /// <summary>
    /// Executes a query against the specified database.
    /// </summary>
    public SproutResponse Execute(string query, string database)
    {
        var dbName = string.Create(database.Length, database, static (span, db) =>
        {
            db.AsSpan().CopyTo(span);
            for (var i = 0; i < span.Length; i++)
                span[i] = char.ToLowerInvariant(span[i]);
        });

        if (!IsValidName(dbName))
            return ResponseHelper.Error(query, ErrorCodes.SYNTAX_ERROR,
                $"invalid database name '{database}'");

        var parseResult = QueryParser.Parse(query);
        if (!parseResult.Success)
            return ResponseHelper.ParseError(parseResult);

        var dbPath = Path.Combine(_dataDirectory, dbName);

        return parseResult.Query switch
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

    // ── Table handle cache ──────────────────────────────────

    private TableHandle GetOrOpenTable(string tablePath)
    {
        if (!_tables.TryGetValue(tablePath, out var handle))
        {
            handle = TableHandle.Open(tablePath);
            _tables[tablePath] = handle;
        }
        return handle;
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

    public void Dispose()
    {
        foreach (var table in _tables.Values)
            table.Dispose();
        _tables.Clear();
    }
}

namespace SproutDB.Core;

/// <summary>
/// Lightweight wrapper that binds a <see cref="SproutEngine"/> to a database name.
/// </summary>
internal sealed class SproutDatabase : ISproutDatabase
{
    private readonly SproutEngine _engine;

    public string Name { get; }

    public SproutDatabase(SproutEngine engine, string name)
    {
        _engine = engine;
        Name = name;
    }

    public SproutResponse Query(string query) => _engine.Execute(query, Name);

    public IDisposable OnChange(string table, Action<SproutResponse> callback)
        => _engine.ChangeNotifier.Subscribe(Name, table, callback);

    internal SproutResponse QueryInternal(string query) => _engine.ExecuteInternal(query, Name);

    public void SaveQuery(string name, string query, bool pinned = false)
    {
        // Ensure _saved_queries table exists
        var describe = _engine.ExecuteInternal("describe _saved_queries", Name);
        if (describe.Operation == SproutOperation.Error)
            _engine.ExecuteInternal("create table _saved_queries (name string 200, query string 4000, pinned bool)", Name);

        var escapedName = name.Replace("'", "\\'");
        var escapedQuery = query.Replace("'", "\\'");
        _engine.ExecuteInternal(
            $"upsert _saved_queries {{ name: '{escapedName}', query: '{escapedQuery}', pinned: {(pinned ? "true" : "false")} }} on name",
            Name);
    }
}

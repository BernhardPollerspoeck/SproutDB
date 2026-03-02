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
}

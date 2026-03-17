namespace SproutDB.Core.Admin;

/// <summary>
/// Singleton service that holds the state of all query editor tabs.
/// Survives page navigation (Query → Admin → back to Query).
/// </summary>
public sealed class QueryWorkspaceState
{
    public List<QueryTab> Tabs { get; } = [new QueryTab()];
    public int ActiveTabIndex { get; set; }

    public QueryTab ActiveTab => Tabs[ActiveTabIndex];

    public QueryTab AddTab(string? title = null, string? query = null, string? database = null, string? savedName = null)
    {
        var tab = new QueryTab
        {
            Title = title ?? $"Query {Tabs.Count + 1}",
            QueryText = query ?? string.Empty,
            Database = database ?? string.Empty,
            SavedName = savedName,
        };
        if (query is not null)
            tab.OriginalText = query;
        Tabs.Add(tab);
        ActiveTabIndex = Tabs.Count - 1;
        return tab;
    }

    public void CloseTab(int index)
    {
        if (Tabs.Count <= 1)
        {
            // Can't close last tab — reset it instead
            Tabs[0] = new QueryTab();
            ActiveTabIndex = 0;
            return;
        }

        Tabs.RemoveAt(index);
        if (ActiveTabIndex >= Tabs.Count)
            ActiveTabIndex = Tabs.Count - 1;
        else if (ActiveTabIndex > index)
            ActiveTabIndex--;
    }

    private int _nextNumber = 2; // "Query 1" is created in the constructor

    public string NextTitle() => $"Query {_nextNumber++}";
}

/// <summary>
/// State for a single query editor tab.
/// </summary>
public sealed class QueryTab
{
    public string Id { get; init; } = Guid.NewGuid().ToString("N");
    public string Title { get; set; } = "Query 1";
    public string QueryText { get; set; } = string.Empty;
    public string Database { get; set; } = string.Empty;

    /// <summary>
    /// Name in _saved_queries if this tab was saved/loaded. Null = unsaved.
    /// </summary>
    public string? SavedName { get; set; }

    /// <summary>
    /// The text as it was when saved/loaded. Used for change detection (● indicator).
    /// </summary>
    public string OriginalText { get; set; } = string.Empty;

    /// <summary>True if the query text differs from the saved/original text.</summary>
    public bool HasChanges => QueryText != OriginalText;

    // Result state — persists when switching tabs
    public List<SproutResponse>? LastResults { get; set; }
    public SproutResponse? LastResponse { get; set; }
    public List<Dictionary<string, object?>>? DisplayData { get; set; }
    public List<ColumnMeta> Columns { get; set; } = [];
    public double ElapsedMs { get; set; }
    public int RowCount { get; set; }
    public int PayloadBytes { get; set; }
    public string ActiveResultTab { get; set; } = "results";
    public string? SortColumn { get; set; }
    public bool SortDesc { get; set; }
    public bool HasIdColumn { get; set; }
    public bool CanEdit { get; set; }
}

public sealed class ColumnMeta
{
    public string Name { get; set; } = string.Empty;
    public string Type { get; set; } = string.Empty;
}

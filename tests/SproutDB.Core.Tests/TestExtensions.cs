namespace SproutDB.Core.Tests;

/// <summary>
/// Convenience wrapper for the Execute() → List&lt;SproutResponse&gt; breaking change.
/// Tests that don't test multi-query batching use this to get a single response.
/// </summary>
internal static class TestExtensions
{
    public static SproutResponse ExecuteOne(this SproutEngine engine, string query, string database)
        => engine.Execute(query, database)[0];

    public static SproutResponse QueryOne(this ISproutDatabase db, string query)
        => db.Query(query)[0];
}

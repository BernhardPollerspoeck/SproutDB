namespace SproutDB.Core.Tests;

public class IndexMetricsPersistenceTests
{
    [Fact]
    public void Metrics_PersistedAndRestored()
    {
        var dir = Path.Combine(Path.GetTempPath(), $"sproutdb-test-{Guid.NewGuid()}");
        try
        {
            // Phase 1: Create engine, run some queries to generate metrics
            using (var engine = new SproutEngine(dir))
            {
                engine.Execute("create database", "shop");
                engine.Execute("create table users (name string 100, age ubyte)", "shop");
                engine.Execute("upsert users {name: 'Alice', age: 25}", "shop");
                engine.Execute("upsert users {name: 'Bob', age: 30}", "shop");

                // These queries generate where usage metrics
                engine.Execute("get users where name = 'Alice'", "shop");
                engine.Execute("get users where name = 'Bob'", "shop");
                engine.Execute("get users where age = 25", "shop");
            }

            // Phase 2: Re-open engine, check metrics survived
            using (var engine2 = new SproutEngine(dir))
            {
                var r = engine2.Execute("get index_metrics", "_system");
                Assert.NotEqual(SproutOperation.Error, r.Operation);
                Assert.NotNull(r.Data);
                Assert.True(r.Data.Count > 0, "Expected at least one index metric entry");

                // Check that name column has where_hit_count >= 2
                var nameMetric = r.Data.FirstOrDefault(d =>
                    d.TryGetValue("column_name", out var col) && col?.ToString() == "name");
                Assert.NotNull(nameMetric);

                var whereHits = nameMetric.TryGetValue("where_hit_count", out var wh) ? Convert.ToInt64(wh) : 0;
                Assert.True(whereHits >= 2, $"Expected where_hit_count >= 2, got {whereHits}");
            }
        }
        finally
        {
            if (Directory.Exists(dir))
                Directory.Delete(dir, true);
        }
    }

    [Fact]
    public void IndexMetrics_Queryable()
    {
        var dir = Path.Combine(Path.GetTempPath(), $"sproutdb-test-{Guid.NewGuid()}");
        try
        {
            using (var engine = new SproutEngine(dir))
            {
                engine.Execute("create database", "shop");
                engine.Execute("create table items (name string 100)", "shop");
                engine.Execute("upsert items {name: 'Widget'}", "shop");
                engine.Execute("get items where name = 'Widget'", "shop");
            }

            // Re-open to verify metrics were persisted
            using var engine2 = new SproutEngine(dir);
            var r = engine2.Execute("get index_metrics where column_name = 'name'", "_system");

            Assert.NotEqual(SproutOperation.Error, r.Operation);
            Assert.NotNull(r.Data);
            Assert.True(r.Data.Count > 0);
        }
        finally
        {
            if (Directory.Exists(dir))
                Directory.Delete(dir, true);
        }
    }
}

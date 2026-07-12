namespace SproutDB.Core.Tests;

/// <summary>
/// Serial collection for tests that measure process-global resources
/// (WAL flush timing, process-wide file-descriptor counts). These metrics
/// are distorted by any concurrently running test class, so this collection
/// must never run in parallel with the rest of the suite.
/// </summary>
[CollectionDefinition("ResourceMetrics", DisableParallelization = true)]
public class ResourceMetricsCollection;

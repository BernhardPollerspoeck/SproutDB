using System.Diagnostics;
using System.Globalization;
using System.Runtime.InteropServices;

namespace SproutDB.Core;

/// <summary>
/// Reads OS-level resource limits that bound how many databases and tables
/// SproutDB can safely keep open concurrently. Two hard caps matter:
///
/// <list type="bullet">
///   <item><b>File descriptors</b> — <c>ulimit -n</c> on Linux. Each open
///     MMF (Column, BTree, Index, TTL) costs ~2 FDs (FileStream + a dup'ed
///     fd for the mapping). A tenant with 30 tables and 200 columns total
///     can cost ~500 FDs.</item>
///   <item><b>Memory-mapped regions</b> — <c>vm.max_map_count</c> on Linux
///     (default 65536). Each <c>CreateViewAccessor</c> is one VMA. Overflow
///     surfaces as <see cref="OutOfMemoryException"/> in .NET.</item>
/// </list>
///
/// Windows has no equivalent hard cap on either; the Linux values are
/// informational only on that platform.
/// </summary>
public static class SproutSystemLimits
{
    /// <summary>
    /// Reads the current process's soft file-descriptor limit (<c>RLIMIT_NOFILE</c>).
    /// Returns <c>int.MaxValue</c> on platforms without a meaningful cap.
    /// </summary>
    public static int GetMaxFileDescriptors()
    {
        if (!RuntimeInformation.IsOSPlatform(OSPlatform.Linux))
            return int.MaxValue;

        try
        {
            var limitsPath = "/proc/self/limits";
            if (!File.Exists(limitsPath)) return int.MaxValue;

            foreach (var line in File.ReadLines(limitsPath))
            {
                if (!line.StartsWith("Max open files", StringComparison.Ordinal))
                    continue;

                // Format: "Max open files   1024   524288   files"
                var parts = line.Split(' ', StringSplitOptions.RemoveEmptyEntries);
                if (parts.Length < 5) return int.MaxValue;

                if (int.TryParse(parts[3], NumberStyles.Integer, CultureInfo.InvariantCulture, out var soft))
                    return soft;
            }
        }
        catch
        {
            // Never let diagnostics break the engine
        }

        return int.MaxValue;
    }

    /// <summary>
    /// Reads <c>vm.max_map_count</c> on Linux. Returns <c>int.MaxValue</c>
    /// on other platforms or if the value can't be determined.
    /// </summary>
    public static int GetMaxMapCount()
    {
        if (!RuntimeInformation.IsOSPlatform(OSPlatform.Linux))
            return int.MaxValue;

        try
        {
            var path = "/proc/sys/vm/max_map_count";
            if (!File.Exists(path)) return int.MaxValue;

            var text = File.ReadAllText(path).Trim();
            if (int.TryParse(text, NumberStyles.Integer, CultureInfo.InvariantCulture, out var value))
                return value;
        }
        catch { }

        return int.MaxValue;
    }

    /// <summary>
    /// Counts file descriptors currently open by this process. Returns -1
    /// on platforms where the count isn't cheaply available.
    /// </summary>
    public static int GetCurrentFileDescriptorCount()
    {
        if (RuntimeInformation.IsOSPlatform(OSPlatform.Linux))
        {
            try
            {
                var fdDir = "/proc/self/fd";
                if (Directory.Exists(fdDir))
                    return Directory.GetFileSystemEntries(fdDir).Length;
            }
            catch { }
            return -1;
        }

        try
        {
            using var proc = Process.GetCurrentProcess();
            proc.Refresh();
            return proc.HandleCount;
        }
        catch
        {
            return -1;
        }
    }

    /// <summary>
    /// Returns recommended caps for <see cref="SproutEngineSettings.MaxOpenDatabases"/>
    /// and <see cref="SproutEngineSettings.MaxOpenTables"/>, derived from the
    /// current <c>RLIMIT_NOFILE</c>. Uses a 70% budget (the rest goes to WALs,
    /// sockets, logs, etc.), an estimated cost of ~2 FDs per handle, and the
    /// caller's rough shape of the data (<paramref name="avgTablesPerDatabase"/>,
    /// <paramref name="avgHandlesPerTable"/> — handle = column or btree).
    ///
    /// On Windows or when the limit is effectively unbounded, returns the
    /// defaults so behavior is unchanged.
    /// </summary>
    public static (int MaxOpenDatabases, int MaxOpenTables) RecommendCaps(
        int avgTablesPerDatabase = 30,
        int avgHandlesPerTable = 8)
    {
        var fdLimit = GetMaxFileDescriptors();
        if (fdLimit == int.MaxValue || fdLimit <= 0)
            return (128, 512);

        var budget = (int)(fdLimit * 0.7);
        var fdsPerHandle = 2;
        var fdsPerTable = avgHandlesPerTable * fdsPerHandle; // ~16 FDs per table
        var maxTables = Math.Max(8, budget / fdsPerTable);
        var maxDatabases = Math.Max(2, maxTables / Math.Max(1, avgTablesPerDatabase));
        return (maxDatabases, maxTables);
    }
}

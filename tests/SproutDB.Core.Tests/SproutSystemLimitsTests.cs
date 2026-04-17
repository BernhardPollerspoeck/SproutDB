using System.Runtime.InteropServices;

namespace SproutDB.Core.Tests;

public class SproutSystemLimitsTests
{
    [Fact]
    public void GetMaxFileDescriptors_ReturnsPlausibleValue()
    {
        var fd = SproutSystemLimits.GetMaxFileDescriptors();

        if (RuntimeInformation.IsOSPlatform(OSPlatform.Linux))
        {
            // Any Linux system returns a positive number. Ubuntu default soft
            // is 1024, but CI environments often raise it — range is wide.
            Assert.True(fd > 0, $"Expected positive fd limit, got {fd}");
            Assert.True(fd < int.MaxValue, $"fd limit should be concrete on Linux, got {fd}");
        }
        else
        {
            // Non-Linux platforms report int.MaxValue (no meaningful cap here)
            Assert.Equal(int.MaxValue, fd);
        }
    }

    [Fact]
    public void GetMaxMapCount_ReturnsPlausibleValue()
    {
        var mm = SproutSystemLimits.GetMaxMapCount();

        if (RuntimeInformation.IsOSPlatform(OSPlatform.Linux))
        {
            // Linux default is 65536; production hosts often raise it.
            Assert.True(mm >= 1024, $"Expected >= 1024, got {mm}");
        }
        else
        {
            Assert.Equal(int.MaxValue, mm);
        }
    }

    [Fact]
    public void GetCurrentFileDescriptorCount_IsNonNegative()
    {
        var n = SproutSystemLimits.GetCurrentFileDescriptorCount();
        Assert.True(n >= 0, $"Expected >= 0, got {n}");
    }

    [Fact]
    public void RecommendCaps_StaysWithinFdBudget()
    {
        var (maxDbs, maxTables) = SproutSystemLimits.RecommendCaps(
            avgTablesPerDatabase: 30,
            avgHandlesPerTable: 8);

        Assert.True(maxDbs >= 2);
        Assert.True(maxTables >= 8);

        // Roughly: maxTables × 8 × 2 should fit within 70% of ulimit
        var fdLimit = SproutSystemLimits.GetMaxFileDescriptors();
        if (fdLimit < int.MaxValue)
        {
            var estimatedFds = maxTables * 8 * 2;
            Assert.True(estimatedFds <= fdLimit,
                $"Recommendation overshoots fd limit: est={estimatedFds} limit={fdLimit}");
        }
    }

    [Fact]
    public void RecommendCaps_SmallFdLimit_ShrinksCaps()
    {
        // We can't actually change RLIMIT_NOFILE from a test, but verify the
        // recommendation logic is monotonic by checking relative shape.
        var (dbLow, tLow) = SproutSystemLimits.RecommendCaps(30, 8);
        var (dbHigh, tHigh) = SproutSystemLimits.RecommendCaps(30, 1);

        // Fewer handles per table → more tables allowed at same FD budget
        Assert.True(tHigh >= tLow,
            $"Lower handle-per-table should allow more tables: tHigh={tHigh} tLow={tLow}");
        Assert.True(dbHigh >= dbLow);
    }
}

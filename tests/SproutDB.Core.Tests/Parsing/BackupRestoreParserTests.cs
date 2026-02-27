using SproutDB.Core.Parsing;

namespace SproutDB.Core.Tests.Parsing;

public class BackupRestoreParserTests
{
    [Fact]
    public void Backup_Success()
    {
        var result = QueryParser.Parse("backup");

        Assert.True(result.Success);
        Assert.IsType<BackupQuery>(result.Query);
    }

    [Fact]
    public void Backup_CaseInsensitive()
    {
        var result = QueryParser.Parse("BACKUP");

        Assert.True(result.Success);
        Assert.IsType<BackupQuery>(result.Query);
    }

    [Fact]
    public void Backup_ExtraTokens_Error()
    {
        var result = QueryParser.Parse("backup extra");

        Assert.False(result.Success);
        Assert.Contains("expected end of query", result.Errors![0].Message);
    }

    [Fact]
    public void Restore_Success()
    {
        var result = QueryParser.Parse("restore '/path/to/backup.zip'");

        Assert.True(result.Success);
        var q = Assert.IsType<RestoreQuery>(result.Query);
        Assert.Equal("/path/to/backup.zip", q.FilePath);
    }

    [Fact]
    public void Restore_CaseInsensitive()
    {
        var result = QueryParser.Parse("RESTORE 'file.zip'");

        Assert.True(result.Success);
        var q = Assert.IsType<RestoreQuery>(result.Query);
        Assert.Equal("file.zip", q.FilePath);
    }

    [Fact]
    public void Restore_MissingPath_Error()
    {
        var result = QueryParser.Parse("restore");

        Assert.False(result.Success);
        Assert.Contains("backup file path", result.Errors![0].Message);
    }

    [Fact]
    public void Restore_ExtraTokens_Error()
    {
        var result = QueryParser.Parse("restore 'file.zip' extra");

        Assert.False(result.Success);
        Assert.Contains("expected end of query", result.Errors![0].Message);
    }
}

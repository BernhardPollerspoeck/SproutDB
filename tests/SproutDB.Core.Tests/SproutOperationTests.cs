using SproutDB.Core;

namespace SproutDB.Core.Tests;

public class SproutOperationTests
{
    [Theory]
    [InlineData(SproutOperation.Error, 0)]
    [InlineData(SproutOperation.Get, 1)]
    [InlineData(SproutOperation.Upsert, 2)]
    [InlineData(SproutOperation.Delete, 3)]
    [InlineData(SproutOperation.Describe, 4)]
    [InlineData(SproutOperation.CreateTable, 5)]
    [InlineData(SproutOperation.CreateDatabase, 6)]
    [InlineData(SproutOperation.PurgeTable, 7)]
    [InlineData(SproutOperation.PurgeDatabase, 8)]
    [InlineData(SproutOperation.PurgeColumn, 9)]
    [InlineData(SproutOperation.AddColumn, 10)]
    [InlineData(SproutOperation.RenameColumn, 11)]
    [InlineData(SproutOperation.AlterColumn, 12)]
    [InlineData(SproutOperation.CreateIndex, 13)]
    [InlineData(SproutOperation.PurgeIndex, 14)]
    public void Operation_HasCorrectByteValue(SproutOperation operation, byte expected)
    {
        Assert.Equal(expected, (byte)operation);
    }

    [Fact]
    public void Operation_HasExactly15Members()
    {
        var values = Enum.GetValues<SproutOperation>();
        Assert.Equal(15, values.Length);
    }
}

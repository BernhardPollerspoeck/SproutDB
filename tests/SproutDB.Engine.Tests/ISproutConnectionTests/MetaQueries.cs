using SproutDB.Engine.Execution;

namespace SproutDB.Engine.Tests.ISproutConnectionTests;

[TestClass]
public class MetaQueries : ISproutConnectionTestsSetup
{
    [TestMethod]
    public void Test_CreateDatabase_Works()
    {
        // Arrange
        var query = $"create database {TEST_DATABASE}";

        // Act
        var result = _connection.Execute(query);

        // Assert
        Assert.IsTrue(result.Success, $"Failed to create database: {result.Error}");
        Assert.IsNull(result.Error);
        Assert.IsTrue(_server.Databases.ContainsKey(TEST_DATABASE));
    }

    [TestMethod]
    public void Test_CreateTable_Works()
    {
        // Arrange
        var createDbResult = _connection.Execute($"create database {TEST_DATABASE}");
        Assert.IsTrue(createDbResult.Success, $"Failed to create database: {createDbResult.Error}");

        // Act
        var result = _connection.Execute($"create table {USERS_TABLE}");

        // Assert
        Assert.IsTrue(result.Success, $"Failed to create table: {result.Error}");
        Assert.IsNull(result.Error);
        var database = _server.Databases[TEST_DATABASE];
        Assert.IsTrue(database.Tables.ContainsKey(USERS_TABLE));
    }

    [TestMethod]
    public void Test_AddColumn_StringType_Works()
    {
        // Arrange
        var createDbResult = _connection.Execute($"create database {TEST_DATABASE}");
        Assert.IsTrue(createDbResult.Success, $"Failed to create database: {createDbResult.Error}");
        var createTableResult = _connection.Execute($"create table {USERS_TABLE}");
        Assert.IsTrue(createTableResult.Success, $"Failed to create table: {createTableResult.Error}");

        // Act
        var result = _connection.Execute($"add column {USERS_TABLE}.{NAME_COLUMN} {STRING_TYPE}");

        // Assert
        Assert.IsTrue(result.Success, $"Failed to add column: {result.Error}");
        Assert.IsNull(result.Error);
        var database = _server.Databases[TEST_DATABASE];
        var table = database.Tables[USERS_TABLE];
        Assert.IsTrue(table.Columns.ContainsKey(NAME_COLUMN));
    }

    [TestMethod]
    public void Test_AddColumn_NumberType_Works()
    {
        // Arrange
        var createDbResult = _connection.Execute($"create database {TEST_DATABASE}");
        Assert.IsTrue(createDbResult.Success);
        var createTableResult = _connection.Execute($"create table {USERS_TABLE}");
        Assert.IsTrue(createTableResult.Success);

        // Act
        var result = _connection.Execute($"add column {USERS_TABLE}.{AGE_COLUMN} {NUMBER_TYPE}");

        // Assert
        Assert.IsTrue(result.Success, $"Failed to add column: {result.Error}");
        Assert.IsNull(result.Error);
        var database = _server.Databases[TEST_DATABASE];
        var table = database.Tables[USERS_TABLE];
        Assert.IsTrue(table.Columns.ContainsKey(AGE_COLUMN));
    }

    [TestMethod]
    public void Test_WidenColumn_NumberToMixed_Works()
    {
        // Arrange
        var createDbResult = _connection.Execute($"create database {TEST_DATABASE}");
        Assert.IsTrue(createDbResult.Success);
        var createTableResult = _connection.Execute($"create table {USERS_TABLE}");
        Assert.IsTrue(createTableResult.Success);
        var addColumnResult = _connection.Execute($"add column {USERS_TABLE}.{AGE_COLUMN} {NUMBER_TYPE}");
        Assert.IsTrue(addColumnResult.Success);

        // Act
        var result = _connection.Execute($"add column {USERS_TABLE}.{AGE_COLUMN} {MIXED_TYPE}");

        // Assert
        Assert.IsTrue(result.Success, $"Failed to widen column: {result.Error}");
        Assert.IsNull(result.Error);
        var database = _server.Databases[TEST_DATABASE];
        var table = database.Tables[USERS_TABLE];
        Assert.IsTrue(table.Columns.ContainsKey(AGE_COLUMN));
        Assert.AreEqual(EColumnType.Mixed, table.Columns[AGE_COLUMN].Type);
    }

    [TestMethod]
    public void Test_AddColumn_BooleanType_Works()
    {
        // Arrange
        var createDbResult = _connection.Execute($"create database {TEST_DATABASE}");
        Assert.IsTrue(createDbResult.Success);
        var createTableResult = _connection.Execute($"create table {USERS_TABLE}");
        Assert.IsTrue(createTableResult.Success);

        // Act
        var result = _connection.Execute($"add column {USERS_TABLE}.{ACTIVE_COLUMN} {BOOLEAN_TYPE}");

        // Assert
        Assert.IsTrue(result.Success, $"Failed to add column: {result.Error}");
        Assert.IsNull(result.Error);
        var database = _server.Databases[TEST_DATABASE];
        var table = database.Tables[USERS_TABLE];
        Assert.IsTrue(table.Columns.ContainsKey(ACTIVE_COLUMN));
    }

    [TestMethod]
    public void Test_DropTable_Works()
    {
        // Arrange
        var createDbResult = _connection.Execute($"create database {TEST_DATABASE}");
        Assert.IsTrue(createDbResult.Success);
        var createTableResult = _connection.Execute($"create table {OLD_TABLE}");
        Assert.IsTrue(createTableResult.Success);

        // Act
        var result = _connection.Execute($"drop table {OLD_TABLE}");

        // Assert
        Assert.IsTrue(result.Success, $"Failed to drop table: {result.Error}");
        Assert.IsNull(result.Error);
        var database = _server.Databases[TEST_DATABASE];
        Assert.IsFalse(database.Tables.ContainsKey(OLD_TABLE));
    }

}

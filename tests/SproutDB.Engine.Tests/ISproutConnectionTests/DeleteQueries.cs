namespace SproutDB.Engine.Tests.ISproutConnectionTests;

[TestClass]
public class DeleteQueries : ISproutConnectionTestsSetup
{


    [TestMethod]
    public void Test_Delete_WithWhereClause_Works()
    {
        // Arrange
        var createDbResult = _connection.Execute($"create database {TEST_DATABASE}");
        Assert.IsTrue(createDbResult.Success);
        var createTableResult = _connection.Execute($"create table {USERS_TABLE}");
        Assert.IsTrue(createTableResult.Success);
        var addNameColumnResult = _connection.Execute($"add column {USERS_TABLE}.{NAME_COLUMN} {STRING_TYPE}");
        Assert.IsTrue(addNameColumnResult.Success);
        var addAgeColumnResult = _connection.Execute($"add column {USERS_TABLE}.{AGE_COLUMN} {NUMBER_TYPE}");
        Assert.IsTrue(addAgeColumnResult.Success);
        var addActiveColumnResult = _connection.Execute($"add column {USERS_TABLE}.{ACTIVE_COLUMN} {BOOLEAN_TYPE}");
        Assert.IsTrue(addActiveColumnResult.Success);
        var aliceUpsertResult = _connection.Execute($"upsert {USERS_TABLE} {{ {NAME_COLUMN}: '{ALICE_NAME}', {AGE_COLUMN}: {ALICE_AGE}, {ACTIVE_COLUMN}: {ALICE_ACTIVE.ToString().ToLower()} }}");
        Assert.IsTrue(aliceUpsertResult.Success);
        var bobUpsertResult = _connection.Execute($"upsert {USERS_TABLE} {{ {NAME_COLUMN}: '{BOB_NAME}', {AGE_COLUMN}: {BOB_AGE}, {ACTIVE_COLUMN}: {BOB_ACTIVE.ToString().ToLower()} }}");
        Assert.IsTrue(bobUpsertResult.Success);

        // Act
        var result = _connection.Execute($"delete {USERS_TABLE} where {NAME_COLUMN} = '{BOB_NAME}'");

        // Assert
        Assert.IsTrue(result.Success, $"Failed to delete record: {result.Error}");
        Assert.IsNull(result.Error);
        Assert.AreEqual(1, result.RowsAffected);
        var database = _server.Databases[TEST_DATABASE];
        var table = database.Tables[USERS_TABLE];
        Assert.AreEqual(1, table.Rows.Count);
        var row = table.Rows.First().Value;
        Assert.AreEqual(ALICE_NAME, row.Fields[NAME_COLUMN]);
        Assert.AreEqual(ALICE_AGE, row.Fields[AGE_COLUMN]);
        Assert.AreEqual(ALICE_ACTIVE, row.Fields[ACTIVE_COLUMN]);
    }

}
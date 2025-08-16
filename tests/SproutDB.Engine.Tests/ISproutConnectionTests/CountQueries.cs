namespace SproutDB.Engine.Tests.ISproutConnectionTests;

[TestClass]
public class CountQueries : ISproutConnectionTestsSetup
{
    [TestMethod]
    public void Test_CountUsers_Works()
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

        // Insert test users with different ages
        var johnResult = _connection.Execute($"upsert {USERS_TABLE} {{ {NAME_COLUMN}: '{JOHN_NAME}', {AGE_COLUMN}: {JOHN_AGE}, {ACTIVE_COLUMN}: {JOHN_ACTIVE.ToString().ToLower()} }}");
        Assert.IsTrue(johnResult.Success);
        var janeResult = _connection.Execute($"upsert {USERS_TABLE} {{ {NAME_COLUMN}: '{JANE_NAME}', {AGE_COLUMN}: {JANE_AGE}, {ACTIVE_COLUMN}: {JANE_ACTIVE.ToString().ToLower()} }}");
        Assert.IsTrue(janeResult.Success);
        var aliceResult = _connection.Execute($"upsert {USERS_TABLE} {{ {NAME_COLUMN}: '{ALICE_NAME}', {AGE_COLUMN}: {ALICE_AGE}, {ACTIVE_COLUMN}: {ALICE_ACTIVE.ToString().ToLower()} }}");
        Assert.IsTrue(aliceResult.Success);
        var bobResult = _connection.Execute($"upsert {USERS_TABLE} {{ {NAME_COLUMN}: '{BOB_NAME}', {AGE_COLUMN}: {BOB_AGE}, {ACTIVE_COLUMN}: {BOB_ACTIVE.ToString().ToLower()} }}");
        Assert.IsTrue(bobResult.Success);

        // Act
        var result = _connection.Execute($"count {USERS_TABLE}");

        // Assert
        Assert.IsTrue(result.Success, $"Failed to count users: {result.Error}");
        Assert.IsNull(result.Error);

        // Verify the returned data
        Assert.IsNotNull(result.Data);
        
        // The result should be a single integer value representing the count
        Assert.IsInstanceOfType(result.Data, typeof(int));
        Assert.AreEqual(4, result.Data);
    }

    [TestMethod]
    public void Test_CountUsers_WhereAgeGreaterThan30_Works()
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

        // Insert test users with different ages
        var johnResult = _connection.Execute($"upsert {USERS_TABLE} {{ {NAME_COLUMN}: '{JOHN_NAME}', {AGE_COLUMN}: {JOHN_AGE}, {ACTIVE_COLUMN}: {JOHN_ACTIVE.ToString().ToLower()} }}");
        Assert.IsTrue(johnResult.Success);
        var janeResult = _connection.Execute($"upsert {USERS_TABLE} {{ {NAME_COLUMN}: '{JANE_NAME}', {AGE_COLUMN}: {JANE_AGE}, {ACTIVE_COLUMN}: {JANE_ACTIVE.ToString().ToLower()} }}");
        Assert.IsTrue(janeResult.Success);
        var aliceResult = _connection.Execute($"upsert {USERS_TABLE} {{ {NAME_COLUMN}: '{ALICE_NAME}', {AGE_COLUMN}: {ALICE_AGE}, {ACTIVE_COLUMN}: {ALICE_ACTIVE.ToString().ToLower()} }}");
        Assert.IsTrue(aliceResult.Success);
        var bobResult = _connection.Execute($"upsert {USERS_TABLE} {{ {NAME_COLUMN}: '{BOB_NAME}', {AGE_COLUMN}: {BOB_AGE}, {ACTIVE_COLUMN}: {BOB_ACTIVE.ToString().ToLower()} }}");
        Assert.IsTrue(bobResult.Success);

        // Calculate expected count for users with age > 30
        var expectedCount = 0;
        if (JOHN_AGE > 30) expectedCount++;
        if (JANE_AGE > 30) expectedCount++;
        if (ALICE_AGE > 30) expectedCount++;
        if (BOB_AGE > 30) expectedCount++;

        // Act
        var result = _connection.Execute($"count {USERS_TABLE} where {AGE_COLUMN} > 30");

        // Assert
        Assert.IsTrue(result.Success, $"Failed to count users where age > 30: {result.Error}");
        Assert.IsNull(result.Error);

        // Verify the returned data
        Assert.IsNotNull(result.Data);
        
        // The result should be a single integer value representing the count
        Assert.IsInstanceOfType(result.Data, typeof(int));
        Assert.AreEqual(expectedCount, result.Data);
        
        // We can also directly verify against our known test data
        // ALICE_AGE = 42, BOB_AGE = 38, JOHN_AGE = 30, JANE_AGE = 25
        // Only Alice and Bob have age > 30
        Assert.AreEqual(2, result.Data);
    }
}
using SproutDB.Engine.Core;

namespace SproutDB.Engine.Tests.ISproutConnectionTests;

[TestClass]
public class GetPaginationQueries : ISproutConnectionTestsSetup
{
    [TestMethod]
    public void Test_GetUsers_WithPagination_Works()
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

        // Insert 10 test users to verify pagination
        for (var i = 1; i <= 10; i++)
        {
            var userResult = _connection.Execute(
                $"upsert {USERS_TABLE} {{ {NAME_COLUMN}: 'User {i}', {AGE_COLUMN}: {20 + i}, {ACTIVE_COLUMN}: {(i % 2 == 0).ToString().ToLower()} }}");
            Assert.IsTrue(userResult.Success);
        }

        // Act
        var result = _connection.Execute($"get {USERS_TABLE} page 1 of size 5");

        // Assert
        Assert.IsTrue(result.Success, $"Failed to get users with pagination: {result.Error}");
        Assert.IsNull(result.Error);

        // Verify the returned data
        Assert.IsNotNull(result.Data);
        var resultRows = result.Data as IList<Row>;
        Assert.IsNotNull(resultRows);

        // Should return only the first 5 users
        Assert.AreEqual(5, resultRows!.Count);

        // Verify that we have the first 5 users
        // Note: Without specific ordering, we'll just verify the count
        // Since there's no order by clause, we don't know the exact order of the results
        // We'll just check that we have 5 different users
        var uniqueUsers = new HashSet<string>(
            resultRows.Select(r => r.Fields[NAME_COLUMN]?.ToString() ?? string.Empty));
        Assert.AreEqual(5, uniqueUsers.Count);
    }

    [TestMethod]
    public void Test_GetUsers_WithPaginationAndOrdering_Works()
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

        // Insert test users with different ages to verify ordering and pagination
        var userAges = new[] { 25, 30, 35, 40, 45, 50, 55, 60, 65, 70 };
        for (var i = 0; i < 10; i++)
        {
            var userResult = _connection.Execute(
                $"upsert {USERS_TABLE} {{ {NAME_COLUMN}: 'User {i + 1}', {AGE_COLUMN}: {userAges[i]}, {ACTIVE_COLUMN}: {(i % 2 == 0).ToString().ToLower()} }}");
            Assert.IsTrue(userResult.Success);
        }

        // Act - get the second page (users 6-10) ordered by age descending
        var result = _connection.Execute($"get {USERS_TABLE} order by {AGE_COLUMN} desc page 2 of size 5");

        // Assert
        Assert.IsTrue(result.Success, $"Failed to get users with pagination and ordering: {result.Error}");
        Assert.IsNull(result.Error);

        // Verify the returned data
        Assert.IsNotNull(result.Data);
        var resultRows = result.Data as IList<Row>;
        Assert.IsNotNull(resultRows);

        // Should return 5 users (the second page)
        Assert.AreEqual(5, resultRows!.Count);

        // Verify that we have the second page of users sorted by age in descending order
        // We should have users with ages: 45, 40, 35, 30, 25
        var expectedAges = new[] { 45d, 40d, 35d, 30d, 25d };
        
        for (var i = 0; i < resultRows.Count; i++)
        {
            Assert.AreEqual(expectedAges[i], resultRows[i].Fields[AGE_COLUMN]);
        }
    }
}
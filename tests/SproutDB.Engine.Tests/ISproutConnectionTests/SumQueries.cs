using SproutDB.Engine.Core;

namespace SproutDB.Engine.Tests.ISproutConnectionTests;

[TestClass]
public class SumQueries : ISproutConnectionTestsSetup
{
    [TestMethod]
    public void Test_SumUsersAge_Works()
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

        // Calculate expected sum of ages
        var expectedSum = JOHN_AGE + JANE_AGE + ALICE_AGE + BOB_AGE;

        // Act
        var result = _connection.Execute($"sum {USERS_TABLE}.{AGE_COLUMN}");

        // Assert
        Assert.IsTrue(result.Success, $"Failed to calculate sum of ages: {result.Error}");
        Assert.IsNull(result.Error);

        // Verify the returned data
        Assert.IsNotNull(result.Data);
        
        Assert.IsInstanceOfType(result.Data, typeof(int));
        Assert.AreEqual(expectedSum, result.Data);
    }

    [TestMethod]
    public void Test_SumUsersAge_WhereActiveIsTrue_Works()
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

        // Insert test users with different active states
        var johnResult = _connection.Execute($"upsert {USERS_TABLE} {{ {NAME_COLUMN}: '{JOHN_NAME}', {AGE_COLUMN}: {JOHN_AGE}, {ACTIVE_COLUMN}: true }}");
        Assert.IsTrue(johnResult.Success);
        var janeResult = _connection.Execute($"upsert {USERS_TABLE} {{ {NAME_COLUMN}: '{JANE_NAME}', {AGE_COLUMN}: {JANE_AGE}, {ACTIVE_COLUMN}: true }}");
        Assert.IsTrue(janeResult.Success);
        var aliceResult = _connection.Execute($"upsert {USERS_TABLE} {{ {NAME_COLUMN}: '{ALICE_NAME}', {AGE_COLUMN}: {ALICE_AGE}, {ACTIVE_COLUMN}: false }}"); // Inactive
        Assert.IsTrue(aliceResult.Success);
        var bobResult = _connection.Execute($"upsert {USERS_TABLE} {{ {NAME_COLUMN}: '{BOB_NAME}', {AGE_COLUMN}: {BOB_AGE}, {ACTIVE_COLUMN}: true }}");
        Assert.IsTrue(bobResult.Success);

        // Calculate expected sum of ages where active is true
        var expectedSum = JOHN_AGE + JANE_AGE + BOB_AGE; // Excluding Alice who is inactive

        // Act
        var result = _connection.Execute($"sum {USERS_TABLE}.{AGE_COLUMN} where {ACTIVE_COLUMN} = true");

        // Assert
        Assert.IsTrue(result.Success, $"Failed to calculate sum of ages where active = true: {result.Error}");
        Assert.IsNull(result.Error);

        // Verify the returned data
        Assert.IsNotNull(result.Data);
        
        Assert.IsInstanceOfType(result.Data, typeof(int));
        Assert.AreEqual(expectedSum, result.Data);
    }

    [TestMethod]
    public void Test_SumDecimalValues_MaintainsDecimalType()
    {
        // Arrange
        var createDbResult = _connection.Execute($"create database {TEST_DATABASE}");
        Assert.IsTrue(createDbResult.Success);
        var createTableResult = _connection.Execute($"create table {ORDERS_TABLE}");
        Assert.IsTrue(createTableResult.Success);
        var addTotalColumnResult = _connection.Execute($"add column {ORDERS_TABLE}.{TOTAL_COLUMN} {NUMBER_TYPE}");
        Assert.IsTrue(addTotalColumnResult.Success);

        // Insert orders with decimal values
        var order1Result = _connection.Execute($"upsert {ORDERS_TABLE} {{ {TOTAL_COLUMN}: 123.45 }}");
        Assert.IsTrue(order1Result.Success);
        var order2Result = _connection.Execute($"upsert {ORDERS_TABLE} {{ {TOTAL_COLUMN}: 67.89 }}");
        Assert.IsTrue(order2Result.Success);
        var order3Result = _connection.Execute($"upsert {ORDERS_TABLE} {{ {TOTAL_COLUMN}: 246.90 }}");
        Assert.IsTrue(order3Result.Success);

        // Calculate expected sum of order totals
        var expectedSum = 123.45 + 67.89 + 246.90;

        // Act
        var result = _connection.Execute($"sum {ORDERS_TABLE}.{TOTAL_COLUMN}");

        // Assert
        Assert.IsTrue(result.Success, $"Failed to calculate sum of decimal values: {result.Error}");
        Assert.IsNull(result.Error);

        // Verify the returned data
        Assert.IsNotNull(result.Data);
        
        // Sum of decimal values should maintain decimal precision
        Assert.IsInstanceOfType(result.Data, typeof(double));
        Assert.AreEqual(expectedSum, (double)result.Data, 0.001); // Allow small epsilon for floating point comparison
    }

    [TestMethod]
    public void Test_SumIntegerValues_PromotesToLongOnOverflow()
    {
        // Arrange
        var createDbResult = _connection.Execute($"create database {TEST_DATABASE}");
        Assert.IsTrue(createDbResult.Success);
        var createTableResult = _connection.Execute($"create table {USERS_TABLE}");
        Assert.IsTrue(createTableResult.Success);
        var addValueColumnResult = _connection.Execute($"add column {USERS_TABLE}.value {NUMBER_TYPE}");
        Assert.IsTrue(addValueColumnResult.Success);

        // Use values that will cause an integer overflow
        int maxInt = int.MaxValue;
        
        // Insert values that will sum to more than int.MaxValue
        var row1Result = _connection.Execute($"upsert {USERS_TABLE} {{ value: {maxInt} }}");
        Assert.IsTrue(row1Result.Success);
        var row2Result = _connection.Execute($"upsert {USERS_TABLE} {{ value: {maxInt} }}");
        Assert.IsTrue(row2Result.Success);
        var row3Result = _connection.Execute($"upsert {USERS_TABLE} {{ value: 10 }}");
        Assert.IsTrue(row3Result.Success);

        // Calculate expected sum (which exceeds int.MaxValue)
        long expectedSum = (long)maxInt + maxInt + 10;

        // Act
        var result = _connection.Execute($"sum {USERS_TABLE}.value");

        // Assert
        Assert.IsTrue(result.Success, $"Failed to calculate sum that overflows int: {result.Error}");
        Assert.IsNull(result.Error);

        // Verify the returned data
        Assert.IsNotNull(result.Data);
        
        // Result should be promoted to long to handle the overflow
        Assert.IsInstanceOfType(result.Data, typeof(long));
        Assert.AreEqual(expectedSum, result.Data);
    }

    [TestMethod]
    public void Test_SumMixedNumericTypes_PromotesAppropriately()
    {
        // Arrange
        var createDbResult = _connection.Execute($"create database {TEST_DATABASE}");
        Assert.IsTrue(createDbResult.Success);
        var createTableResult = _connection.Execute($"create table {ORDERS_TABLE}");
        Assert.IsTrue(createTableResult.Success);
        var addAmountColumnResult = _connection.Execute($"add column {ORDERS_TABLE}.amount {NUMBER_TYPE}");
        Assert.IsTrue(addAmountColumnResult.Success);

        // Insert mixed numeric types - integers and decimals
        var order1Result = _connection.Execute($"upsert {ORDERS_TABLE} {{ amount: 100 }}"); // Integer
        Assert.IsTrue(order1Result.Success);
        var order2Result = _connection.Execute($"upsert {ORDERS_TABLE} {{ amount: 150.75 }}"); // Decimal
        Assert.IsTrue(order2Result.Success);
        var order3Result = _connection.Execute($"upsert {ORDERS_TABLE} {{ amount: 200 }}"); // Integer
        Assert.IsTrue(order3Result.Success);

        // Calculate expected sum
        var expectedSum = 100 + 150.75 + 200;

        // Act
        var result = _connection.Execute($"sum {ORDERS_TABLE}.amount");

        // Assert
        Assert.IsTrue(result.Success, $"Failed to calculate sum of mixed numeric types: {result.Error}");
        Assert.IsNull(result.Error);

        // Verify the returned data
        Assert.IsNotNull(result.Data);
        
        // Result should be promoted to double to handle mixed types
        Assert.IsInstanceOfType(result.Data, typeof(double));
        Assert.AreEqual(expectedSum, (double)result.Data, 0.001); // Allow small epsilon for floating point comparison
    }

    [TestMethod]
    public void Test_SumWithNullValues_IgnoresNulls()
    {
        // Arrange
        var createDbResult = _connection.Execute($"create database {TEST_DATABASE}");
        Assert.IsTrue(createDbResult.Success);
        var createTableResult = _connection.Execute($"create table {USERS_TABLE}");
        Assert.IsTrue(createTableResult.Success);
        var addScoreColumnResult = _connection.Execute($"add column {USERS_TABLE}.score {NUMBER_TYPE}");
        Assert.IsTrue(addScoreColumnResult.Success);

        // Insert some rows with null values
        var row1Result = _connection.Execute($"upsert {USERS_TABLE} {{ score: 10 }}");
        Assert.IsTrue(row1Result.Success);
        var row2Result = _connection.Execute($"upsert {USERS_TABLE} {{ score: null }}"); // Null value
        Assert.IsTrue(row2Result.Success);
        var row3Result = _connection.Execute($"upsert {USERS_TABLE} {{ score: 30 }}");
        Assert.IsTrue(row3Result.Success);

        // Calculate expected sum (ignoring nulls)
        var expectedSum = 10 + 30;

        // Act
        var result = _connection.Execute($"sum {USERS_TABLE}.score");

        // Assert
        Assert.IsTrue(result.Success, $"Failed to calculate sum with null values: {result.Error}");
        Assert.IsNull(result.Error);

        // Verify the returned data
        Assert.IsNotNull(result.Data);
        
        // Result should exclude null values
        Assert.IsInstanceOfType(result.Data, typeof(int));
        Assert.AreEqual(expectedSum, result.Data);
    }

    [TestMethod]
    public void Test_SumEmptyTable_ReturnsZero()
    {
        // Arrange
        var createDbResult = _connection.Execute($"create database {TEST_DATABASE}");
        Assert.IsTrue(createDbResult.Success);
        var createTableResult = _connection.Execute($"create table {USERS_TABLE}");
        Assert.IsTrue(createTableResult.Success);
        var addAgeColumnResult = _connection.Execute($"add column {USERS_TABLE}.{AGE_COLUMN} {NUMBER_TYPE}");
        Assert.IsTrue(addAgeColumnResult.Success);

        // No rows inserted - empty table

        // Act
        var result = _connection.Execute($"sum {USERS_TABLE}.{AGE_COLUMN}");

        // Assert
        Assert.IsTrue(result.Success, $"Failed to calculate sum on empty table: {result.Error}");
        Assert.IsNull(result.Error);

        // Verify the returned data
        Assert.IsNotNull(result.Data);
        
        // Result should be 0 for an empty table
        Assert.IsInstanceOfType(result.Data, typeof(int));
        Assert.AreEqual(0, result.Data);
    }
}
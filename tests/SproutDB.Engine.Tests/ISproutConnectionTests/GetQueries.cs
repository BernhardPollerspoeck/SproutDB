using SproutDB.Engine.Core;

namespace SproutDB.Engine.Tests.ISproutConnectionTests;

[TestClass]
public class GetQueries : ISproutConnectionTestsSetup
{
    [TestMethod]
    public void Test_GetUsers_ReturnsAllUsers()
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

        // Insert test users
        var aliceResult = _connection.Execute($"upsert {USERS_TABLE} {{ {NAME_COLUMN}: '{ALICE_NAME}', {AGE_COLUMN}: {ALICE_AGE}, {ACTIVE_COLUMN}: {ALICE_ACTIVE.ToString().ToLower()} }}");
        Assert.IsTrue(aliceResult.Success);
        var bobResult = _connection.Execute($"upsert {USERS_TABLE} {{ {NAME_COLUMN}: '{BOB_NAME}', {AGE_COLUMN}: {BOB_AGE}, {ACTIVE_COLUMN}: {BOB_ACTIVE.ToString().ToLower()} }}");
        Assert.IsTrue(bobResult.Success);

        // Act
        var result = _connection.Execute($"get {USERS_TABLE}");

        // Assert
        Assert.IsTrue(result.Success, $"Failed to get users: {result.Error}");
        Assert.IsNull(result.Error);

        // Verify the returned data
        Assert.IsNotNull(result.Data);
        var resultRows = result.Data as IList<Row>;
        Assert.IsNotNull(resultRows);
        Assert.AreEqual(2, resultRows!.Count);

        // Verify Alice's data is present
        var aliceRow = resultRows.FirstOrDefault(r => r.Fields[NAME_COLUMN]?.Equals(ALICE_NAME) is true);
        Assert.IsNotNull(aliceRow);
        Assert.AreEqual(ALICE_AGE, aliceRow.Fields[AGE_COLUMN]);
        Assert.AreEqual(ALICE_ACTIVE, aliceRow.Fields[ACTIVE_COLUMN]);

        // Verify Bob's data is present
        var bobRow = resultRows.FirstOrDefault(r => r.Fields[NAME_COLUMN]?.Equals(BOB_NAME) is true);
        Assert.IsNotNull(bobRow);
        Assert.AreEqual(BOB_AGE, bobRow.Fields[AGE_COLUMN]);
        Assert.AreEqual(BOB_ACTIVE, bobRow.Fields[ACTIVE_COLUMN]);
    }

    [TestMethod]
    public void Test_GetUsers_SelectSpecificColumns_Works()
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

        // Insert test users
        var aliceResult = _connection.Execute($"upsert {USERS_TABLE} {{ {NAME_COLUMN}: '{ALICE_NAME}', {AGE_COLUMN}: {ALICE_AGE}, {ACTIVE_COLUMN}: {ALICE_ACTIVE.ToString().ToLower()} }}");
        Assert.IsTrue(aliceResult.Success);
        var bobResult = _connection.Execute($"upsert {USERS_TABLE} {{ {NAME_COLUMN}: '{BOB_NAME}', {AGE_COLUMN}: {BOB_AGE}, {ACTIVE_COLUMN}: {BOB_ACTIVE.ToString().ToLower()} }}");
        Assert.IsTrue(bobResult.Success);

        // Act
        var result = _connection.Execute($"get {USERS_TABLE} select {NAME_COLUMN}, {AGE_COLUMN}");

        // Assert
        Assert.IsTrue(result.Success, $"Failed to get users with specific columns: {result.Error}");
        Assert.IsNull(result.Error);

        // Verify the returned data
        Assert.IsNotNull(result.Data);
        var resultRows = result.Data as IList<Row>;
        Assert.IsNotNull(resultRows);
        Assert.AreEqual(2, resultRows!.Count);

        // Verify only requested columns are present
        var firstRow = resultRows[0];
        Assert.IsTrue(firstRow.Fields.ContainsKey(NAME_COLUMN));
        Assert.IsTrue(firstRow.Fields.ContainsKey(AGE_COLUMN));
        Assert.IsFalse(firstRow.Fields.ContainsKey(ACTIVE_COLUMN));

        // Verify Alice's data is present with correct columns
        var aliceRow = resultRows.FirstOrDefault(r => r.Fields[NAME_COLUMN]?.Equals(ALICE_NAME) is true);
        Assert.IsNotNull(aliceRow);
        Assert.AreEqual(ALICE_AGE, aliceRow.Fields[AGE_COLUMN]);

        // Verify Bob's data is present with correct columns
        var bobRow = resultRows.FirstOrDefault(r => r.Fields[NAME_COLUMN]?.Equals(BOB_NAME) is true);
        Assert.IsNotNull(bobRow);
        Assert.AreEqual(BOB_AGE, bobRow.Fields[AGE_COLUMN]);
    }

    [TestMethod]
    public void Test_GetUsers_WithAlias_SelectSpecificColumns_Works()
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

        // Insert test users
        var aliceResult = _connection.Execute($"upsert {USERS_TABLE} {{ {NAME_COLUMN}: '{ALICE_NAME}', {AGE_COLUMN}: {ALICE_AGE}, {ACTIVE_COLUMN}: {ALICE_ACTIVE.ToString().ToLower()} }}");
        Assert.IsTrue(aliceResult.Success);
        var bobResult = _connection.Execute($"upsert {USERS_TABLE} {{ {NAME_COLUMN}: '{BOB_NAME}', {AGE_COLUMN}: {BOB_AGE}, {ACTIVE_COLUMN}: {BOB_ACTIVE.ToString().ToLower()} }}");
        Assert.IsTrue(bobResult.Success);

        // Act
        var result = _connection.Execute($"get {USERS_TABLE} as u select u.{NAME_COLUMN}, u.{AGE_COLUMN}");

        // Assert
        Assert.IsTrue(result.Success, $"Failed to get users with alias and specific columns: {result.Error}");
        Assert.IsNull(result.Error);

        // Verify the returned data
        Assert.IsNotNull(result.Data);
        var resultRows = result.Data as IList<Row>;
        Assert.IsNotNull(resultRows);
        Assert.AreEqual(2, resultRows!.Count);

        // Verify only requested columns are present
        var firstRow = resultRows[0];
        Assert.IsTrue(firstRow.Fields.ContainsKey($"u.{NAME_COLUMN}"));
        Assert.IsTrue(firstRow.Fields.ContainsKey($"u.{AGE_COLUMN}"));
        Assert.IsFalse(firstRow.Fields.ContainsKey($"u.{ACTIVE_COLUMN}"));

        // Verify Alice's data is present with correct columns
        var aliceRow = resultRows.FirstOrDefault(r => r.Fields[$"u.{NAME_COLUMN}"]?.Equals(ALICE_NAME) is true);
        Assert.IsNotNull(aliceRow);
        Assert.AreEqual(ALICE_AGE, aliceRow.Fields[$"u.{AGE_COLUMN}"]);

        // Verify Bob's data is present with correct columns
        var bobRow = resultRows.FirstOrDefault(r => r.Fields[$"u.{NAME_COLUMN}"]?.Equals(BOB_NAME) is true);
        Assert.IsNotNull(bobRow);
        Assert.AreEqual(BOB_AGE, bobRow.Fields[$"u.{AGE_COLUMN}"]);
    }

    [TestMethod]
    public void Test_GetUsers_OrderByAge_Ascending_Works()
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
        var result = _connection.Execute($"get {USERS_TABLE} order by {AGE_COLUMN}");

        // Assert
        Assert.IsTrue(result.Success, $"Failed to get users ordered by age: {result.Error}");
        Assert.IsNull(result.Error);

        // Verify the returned data
        Assert.IsNotNull(result.Data);
        var resultRows = result.Data as IList<Row>;
        Assert.IsNotNull(resultRows);
        Assert.AreEqual(4, resultRows!.Count);

        // Verify rows are ordered by age in ascending order
        // Expected order: Jane (25), John (30), Bob (38), Alice (42)
        Assert.AreEqual(JANE_NAME, resultRows[0].Fields[NAME_COLUMN]);
        Assert.AreEqual(JANE_AGE, resultRows[0].Fields[AGE_COLUMN]);

        Assert.AreEqual(JOHN_NAME, resultRows[1].Fields[NAME_COLUMN]);
        Assert.AreEqual(JOHN_AGE, resultRows[1].Fields[AGE_COLUMN]);

        Assert.AreEqual(BOB_NAME, resultRows[2].Fields[NAME_COLUMN]);
        Assert.AreEqual(BOB_AGE, resultRows[2].Fields[AGE_COLUMN]);

        Assert.AreEqual(ALICE_NAME, resultRows[3].Fields[NAME_COLUMN]);
        Assert.AreEqual(ALICE_AGE, resultRows[3].Fields[AGE_COLUMN]);
    }

    [TestMethod]
    public void Test_GetUsers_OrderByAge_Descending_Works()
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
        var result = _connection.Execute($"get {USERS_TABLE} order by {AGE_COLUMN} desc");

        // Assert
        Assert.IsTrue(result.Success, $"Failed to get users ordered by age descending: {result.Error}");
        Assert.IsNull(result.Error);

        // Verify the returned data
        Assert.IsNotNull(result.Data);
        var resultRows = result.Data as IList<Row>;
        Assert.IsNotNull(resultRows);
        Assert.AreEqual(4, resultRows!.Count);

        // Verify rows are ordered by age in descending order
        // Expected order: Alice (42), Bob (38), John (30), Jane (25)
        Assert.AreEqual(ALICE_NAME, resultRows[0].Fields[NAME_COLUMN]);
        Assert.AreEqual(ALICE_AGE, resultRows[0].Fields[AGE_COLUMN]);

        Assert.AreEqual(BOB_NAME, resultRows[1].Fields[NAME_COLUMN]);
        Assert.AreEqual(BOB_AGE, resultRows[1].Fields[AGE_COLUMN]);

        Assert.AreEqual(JOHN_NAME, resultRows[2].Fields[NAME_COLUMN]);
        Assert.AreEqual(JOHN_AGE, resultRows[2].Fields[AGE_COLUMN]);

        Assert.AreEqual(JANE_NAME, resultRows[3].Fields[NAME_COLUMN]);
        Assert.AreEqual(JANE_AGE, resultRows[3].Fields[AGE_COLUMN]);
    }

    [TestMethod]
    public void Test_GetUsers_OrderByMultipleColumns_WithDifferentDirections_Works()
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

        // Insert test users with names that start with the same letter but different ages
        // Add users with duplicate first name initial to test multi-column ordering
        var johnResult = _connection.Execute($"upsert {USERS_TABLE} {{ {NAME_COLUMN}: '{JOHN_NAME}', {AGE_COLUMN}: {JOHN_AGE}, {ACTIVE_COLUMN}: {JOHN_ACTIVE.ToString().ToLower()} }}");
        Assert.IsTrue(johnResult.Success);
        var alice1Result = _connection.Execute($"upsert {USERS_TABLE} {{ {NAME_COLUMN}: '{ALICE_NAME}', {AGE_COLUMN}: {ALICE_AGE}, {ACTIVE_COLUMN}: {ALICE_ACTIVE.ToString().ToLower()} }}");
        Assert.IsTrue(alice1Result.Success);
        var bobResult = _connection.Execute($"upsert {USERS_TABLE} {{ {NAME_COLUMN}: '{BOB_NAME}', {AGE_COLUMN}: {BOB_AGE}, {ACTIVE_COLUMN}: {BOB_ACTIVE.ToString().ToLower()} }}");
        Assert.IsTrue(bobResult.Success);
        var alice2Result = _connection.Execute($"upsert {USERS_TABLE} {{ {NAME_COLUMN}: '{ALICE_GREEN_NAME}', {AGE_COLUMN}: {ALICE_GREEN_AGE}, {ACTIVE_COLUMN}: {ALICE_GREEN_ACTIVE.ToString().ToLower()} }}");
        Assert.IsTrue(alice2Result.Success);



        // Act
        var result = _connection.Execute($"get {USERS_TABLE} order by {NAME_COLUMN} asc, {AGE_COLUMN} desc");

        // Assert
        Assert.IsTrue(result.Success, $"Failed to get users ordered by multiple columns: {result.Error}");
        Assert.IsNull(result.Error);

        // Verify the returned data
        Assert.IsNotNull(result.Data);
        var resultRows = result.Data as IList<Row>;
        Assert.IsNotNull(resultRows);
        Assert.AreEqual(4, resultRows!.Count);

        // Verify rows are first ordered by name ascending, then by age descending within same name groups
        // Expected order:
        // 1. Alice Cooper (42) - starts with "A", higher age
        // 2. Alice Green (28) - starts with "A", lower age
        // 3. Bob Johnson (38) - starts with "B"
        // 4. John Doe (30) - starts with "J"

        // First two should be Alice names, with the older Alice first
        Assert.AreEqual(ALICE_NAME, resultRows[0].Fields[NAME_COLUMN]);
        Assert.AreEqual(ALICE_AGE, resultRows[0].Fields[AGE_COLUMN]);

        Assert.AreEqual(ALICE_GREEN_NAME, resultRows[1].Fields[NAME_COLUMN]);
        Assert.AreEqual(ALICE_GREEN_AGE, resultRows[1].Fields[AGE_COLUMN]);

        // Then Bob
        Assert.AreEqual(BOB_NAME, resultRows[2].Fields[NAME_COLUMN]);
        Assert.AreEqual(BOB_AGE, resultRows[2].Fields[AGE_COLUMN]);

        // Then John
        Assert.AreEqual(JOHN_NAME, resultRows[3].Fields[NAME_COLUMN]);
        Assert.AreEqual(JOHN_AGE, resultRows[3].Fields[AGE_COLUMN]);
    }

    [TestMethod]
    public void Test_GetUsers_WhereAgeGreaterThan_Works()
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
        var result = _connection.Execute($"get {USERS_TABLE} where {AGE_COLUMN} > 25");

        // Assert
        Assert.IsTrue(result.Success, $"Failed to get users with age > 25: {result.Error}");
        Assert.IsNull(result.Error);

        // Verify the returned data
        Assert.IsNotNull(result.Data);
        var resultRows = result.Data as IList<Row>;
        Assert.IsNotNull(resultRows);

        // Should return 3 users
        Assert.AreEqual(3, resultRows!.Count);

        // Verify all returned users have age > 25
        foreach (var row in resultRows)
        {
            Assert.IsTrue((double)row.Fields[AGE_COLUMN]! > 25);
        }
    }

    [TestMethod]
    public void Test_GetUsers_WhereAgeGreaterThanOrEqual_Works()
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
        var result = _connection.Execute($"get {USERS_TABLE} where {AGE_COLUMN} >= 30");

        // Assert
        Assert.IsTrue(result.Success, $"Failed to get users with age >= 30: {result.Error}");
        Assert.IsNull(result.Error);

        // Verify the returned data
        Assert.IsNotNull(result.Data);
        var resultRows = result.Data as IList<Row>;
        Assert.IsNotNull(resultRows);

        // Should return John (30), Bob (38), and Alice (42)
        Assert.AreEqual(3, resultRows!.Count);

        // Verify all returned users have age >= 30
        foreach (var row in resultRows)
        {
            Assert.IsTrue((double)row.Fields[AGE_COLUMN]! >= 30);
        }

        // Check specifically for expected users
        var names = resultRows.Select(r => r.Fields[NAME_COLUMN]?.ToString()).ToList();
        Assert.IsTrue(names.Contains(JOHN_NAME));
        Assert.IsTrue(names.Contains(BOB_NAME));
        Assert.IsTrue(names.Contains(ALICE_NAME));
        Assert.IsFalse(names.Contains(JANE_NAME));
    }

    [TestMethod]
    public void Test_GetUsers_WhereAgeLessThan_Works()
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
        var result = _connection.Execute($"get {USERS_TABLE} where {AGE_COLUMN} < 40");

        // Assert
        Assert.IsTrue(result.Success, $"Failed to get users with age < 40: {result.Error}");
        Assert.IsNull(result.Error);

        // Verify the returned data
        Assert.IsNotNull(result.Data);
        var resultRows = result.Data as IList<Row>;
        Assert.IsNotNull(resultRows);

        // Should return Jane (25), John (30), Bob (38)
        Assert.AreEqual(3, resultRows!.Count);

        // Verify all returned users have age < 40
        foreach (var row in resultRows)
        {
            Assert.IsTrue((double)row.Fields[AGE_COLUMN]! < 40);
        }

        // Check specifically for expected users
        var names = resultRows.Select(r => r.Fields[NAME_COLUMN]?.ToString()).ToList();
        Assert.IsTrue(names.Contains(JANE_NAME));
        Assert.IsTrue(names.Contains(JOHN_NAME));
        Assert.IsTrue(names.Contains(BOB_NAME));
        Assert.IsFalse(names.Contains(ALICE_NAME));
    }

    [TestMethod]
    public void Test_GetUsers_WhereAgeLessThanOrEqual_Works()
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
        var result = _connection.Execute($"get {USERS_TABLE} where {AGE_COLUMN} <= 35");

        // Assert
        Assert.IsTrue(result.Success, $"Failed to get users with age <= 35: {result.Error}");
        Assert.IsNull(result.Error);

        // Verify the returned data
        Assert.IsNotNull(result.Data);
        var resultRows = result.Data as IList<Row>;
        Assert.IsNotNull(resultRows);

        // Should return Jane (25) and John (30)
        Assert.AreEqual(2, resultRows!.Count);

        // Verify all returned users have age <= 35
        foreach (var row in resultRows)
        {
            Assert.IsTrue((double)row.Fields[AGE_COLUMN]! <= 35);
        }

        // Check specifically for expected users
        var names = resultRows.Select(r => r.Fields[NAME_COLUMN]?.ToString()).ToList();
        Assert.IsTrue(names.Contains(JANE_NAME));
        Assert.IsTrue(names.Contains(JOHN_NAME));
    }

    [TestMethod]
    public void Test_GetUsers_WhereNameEquals_Works()
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

        // Insert test users
        var johnResult = _connection.Execute($"upsert {USERS_TABLE} {{ {NAME_COLUMN}: '{JOHN_NAME}', {AGE_COLUMN}: {JOHN_AGE}, {ACTIVE_COLUMN}: {JOHN_ACTIVE.ToString().ToLower()} }}");
        Assert.IsTrue(johnResult.Success);
        var janeResult = _connection.Execute($"upsert {USERS_TABLE} {{ {NAME_COLUMN}: '{JANE_NAME}', {AGE_COLUMN}: {JANE_AGE}, {ACTIVE_COLUMN}: {JANE_ACTIVE.ToString().ToLower()} }}");
        Assert.IsTrue(janeResult.Success);

        // Act
        var result = _connection.Execute($"get {USERS_TABLE} where {NAME_COLUMN} = '{JOHN_NAME}'");

        // Assert
        Assert.IsTrue(result.Success, $"Failed to get users with name = '{JOHN_NAME}': {result.Error}");
        Assert.IsNull(result.Error);

        // Verify the returned data
        Assert.IsNotNull(result.Data);
        var resultRows = result.Data as IList<Row>;
        Assert.IsNotNull(resultRows);

        // Should return only John
        Assert.AreEqual(1, resultRows!.Count);
        Assert.AreEqual(JOHN_NAME, resultRows[0].Fields[NAME_COLUMN]);
        Assert.AreEqual(JOHN_AGE, resultRows[0].Fields[AGE_COLUMN]);
        Assert.AreEqual(JOHN_ACTIVE, resultRows[0].Fields[ACTIVE_COLUMN]);
    }

    [TestMethod]
    public void Test_GetUsers_WhereNameNotEquals_Works()
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

        // Insert test users
        var johnResult = _connection.Execute($"upsert {USERS_TABLE} {{ {NAME_COLUMN}: '{JOHN_NAME}', {AGE_COLUMN}: {JOHN_AGE}, {ACTIVE_COLUMN}: {JOHN_ACTIVE.ToString().ToLower()} }}");
        Assert.IsTrue(johnResult.Success);
        var janeResult = _connection.Execute($"upsert {USERS_TABLE} {{ {NAME_COLUMN}: '{JANE_NAME}', {AGE_COLUMN}: {JANE_AGE}, {ACTIVE_COLUMN}: {JANE_ACTIVE.ToString().ToLower()} }}");
        Assert.IsTrue(janeResult.Success);
        var aliceResult = _connection.Execute($"upsert {USERS_TABLE} {{ {NAME_COLUMN}: '{ALICE_NAME}', {AGE_COLUMN}: {ALICE_AGE}, {ACTIVE_COLUMN}: {ALICE_ACTIVE.ToString().ToLower()} }}");
        Assert.IsTrue(aliceResult.Success);

        // Act
        var result = _connection.Execute($"get {USERS_TABLE} where {NAME_COLUMN} != '{JANE_NAME}'");

        // Assert
        Assert.IsTrue(result.Success, $"Failed to get users with name != '{JANE_NAME}': {result.Error}");
        Assert.IsNull(result.Error);

        // Verify the returned data
        Assert.IsNotNull(result.Data);
        var resultRows = result.Data as IList<Row>;
        Assert.IsNotNull(resultRows);

        // Should return John and Alice, but not Jane
        Assert.AreEqual(2, resultRows!.Count);

        // Check that none of the returned rows have Jane's name
        foreach (var row in resultRows)
        {
            Assert.AreNotEqual(JANE_NAME, row.Fields[NAME_COLUMN]);
        }

        // Check specifically for expected users
        var names = resultRows.Select(r => r.Fields[NAME_COLUMN]?.ToString()).ToList();
        Assert.IsTrue(names.Contains(JOHN_NAME));
        Assert.IsTrue(names.Contains(ALICE_NAME));
        Assert.IsFalse(names.Contains(JANE_NAME));
    }

    [TestMethod]
    public void Test_GetUsers_WhereAgeAndActive_Works()
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
        var johnResult = _connection.Execute($"upsert {USERS_TABLE} {{ {NAME_COLUMN}: '{JOHN_NAME}', {AGE_COLUMN}: {JOHN_AGE}, {ACTIVE_COLUMN}: {JOHN_ACTIVE.ToString().ToLower()} }}");
        Assert.IsTrue(johnResult.Success);
        var janeResult = _connection.Execute($"upsert {USERS_TABLE} {{ {NAME_COLUMN}: '{JANE_NAME}', {AGE_COLUMN}: {JANE_AGE}, {ACTIVE_COLUMN}: {JANE_ACTIVE.ToString().ToLower()} }}");
        Assert.IsTrue(janeResult.Success);
        var aliceResult = _connection.Execute($"upsert {USERS_TABLE} {{ {NAME_COLUMN}: '{ALICE_NAME}', {AGE_COLUMN}: {ALICE_AGE}, {ACTIVE_COLUMN}: {ALICE_ACTIVE.ToString().ToLower()} }}");
        Assert.IsTrue(aliceResult.Success);
        var bobResult = _connection.Execute($"upsert {USERS_TABLE} {{ {NAME_COLUMN}: '{BOB_NAME}', {AGE_COLUMN}: {BOB_AGE}, {ACTIVE_COLUMN}: false }}");
        Assert.IsTrue(bobResult.Success);

        // Act
        var result = _connection.Execute($"get {USERS_TABLE} where {AGE_COLUMN} > 25 and {ACTIVE_COLUMN} = true");

        // Assert
        Assert.IsTrue(result.Success, $"Failed to get users with age > 25 and active = true: {result.Error}");
        Assert.IsNull(result.Error);

        // Verify the returned data
        Assert.IsNotNull(result.Data);
        var resultRows = result.Data as IList<Row>;
        Assert.IsNotNull(resultRows);

        // Should return John and Alice (both active and over 25), but not Bob (inactive)
        Assert.AreEqual(2, resultRows!.Count);

        // Verify all returned users have age > 25 and are active
        foreach (var row in resultRows)
        {
            Assert.IsTrue((double)row.Fields[AGE_COLUMN]! > 25);
            Assert.IsTrue((bool)row.Fields[ACTIVE_COLUMN]!);
        }

        // Check specifically for expected users
        var names = resultRows.Select(r => r.Fields[NAME_COLUMN]?.ToString()).ToList();
        Assert.IsTrue(names.Contains(JOHN_NAME));
        Assert.IsTrue(names.Contains(ALICE_NAME));
        Assert.IsFalse(names.Contains(JANE_NAME));
        Assert.IsFalse(names.Contains(BOB_NAME));
    }

    [TestMethod]
    public void Test_GetUsers_WhereNameOr_Works()
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

        // Insert test users
        var johnResult = _connection.Execute($"upsert {USERS_TABLE} {{ {NAME_COLUMN}: '{JOHN_NAME}', {AGE_COLUMN}: {JOHN_AGE}, {ACTIVE_COLUMN}: {JOHN_ACTIVE.ToString().ToLower()} }}");
        Assert.IsTrue(johnResult.Success);
        var janeResult = _connection.Execute($"upsert {USERS_TABLE} {{ {NAME_COLUMN}: '{JANE_NAME}', {AGE_COLUMN}: {JANE_AGE}, {ACTIVE_COLUMN}: {JANE_ACTIVE.ToString().ToLower()} }}");
        Assert.IsTrue(janeResult.Success);
        var aliceResult = _connection.Execute($"upsert {USERS_TABLE} {{ {NAME_COLUMN}: '{ALICE_NAME}', {AGE_COLUMN}: {ALICE_AGE}, {ACTIVE_COLUMN}: {ALICE_ACTIVE.ToString().ToLower()} }}");
        Assert.IsTrue(aliceResult.Success);
        var bobResult = _connection.Execute($"upsert {USERS_TABLE} {{ {NAME_COLUMN}: '{BOB_NAME}', {AGE_COLUMN}: {BOB_AGE}, {ACTIVE_COLUMN}: {BOB_ACTIVE.ToString().ToLower()} }}");
        Assert.IsTrue(bobResult.Success);

        // Act
        var result = _connection.Execute($"get {USERS_TABLE} where {NAME_COLUMN} = '{JOHN_NAME}' or {NAME_COLUMN} = '{JANE_NAME}'");

        // Assert
        Assert.IsTrue(result.Success, $"Failed to get users with name = '{JOHN_NAME}' or name = '{JANE_NAME}': {result.Error}");
        Assert.IsNull(result.Error);

        // Verify the returned data
        Assert.IsNotNull(result.Data);
        var resultRows = result.Data as IList<Row>;
        Assert.IsNotNull(resultRows);

        // Should return John and Jane only
        Assert.AreEqual(2, resultRows!.Count);

        // Check specifically for expected users
        var names = resultRows.Select(r => r.Fields[NAME_COLUMN]?.ToString()).ToList();
        Assert.IsTrue(names.Contains(JOHN_NAME));
        Assert.IsTrue(names.Contains(JANE_NAME));
        Assert.IsFalse(names.Contains(ALICE_NAME));
        Assert.IsFalse(names.Contains(BOB_NAME));
    }

    [TestMethod]
    public void Test_GetUsers_WhereNotActive_Works()
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
        var johnResult = _connection.Execute($"upsert {USERS_TABLE} {{ {NAME_COLUMN}: '{JOHN_NAME}', {AGE_COLUMN}: {JOHN_AGE}, {ACTIVE_COLUMN}: {JOHN_ACTIVE.ToString().ToLower()} }}");
        Assert.IsTrue(johnResult.Success);
        var janeResult = _connection.Execute($"upsert {USERS_TABLE} {{ {NAME_COLUMN}: '{JANE_NAME}', {AGE_COLUMN}: {JANE_AGE}, {ACTIVE_COLUMN}: {JANE_ACTIVE.ToString().ToLower()} }}");
        Assert.IsTrue(janeResult.Success);
        var aliceResult = _connection.Execute($"upsert {USERS_TABLE} {{ {NAME_COLUMN}: '{ALICE_NAME}', {AGE_COLUMN}: {ALICE_AGE}, {ACTIVE_COLUMN}: false }}");
        Assert.IsTrue(aliceResult.Success);
        var bobResult = _connection.Execute($"upsert {USERS_TABLE} {{ {NAME_COLUMN}: '{BOB_NAME}', {AGE_COLUMN}: {BOB_AGE}, {ACTIVE_COLUMN}: {BOB_ACTIVE.ToString().ToLower()} }}");
        Assert.IsTrue(bobResult.Success);

        // Act
        var result = _connection.Execute($"get {USERS_TABLE} where not {ACTIVE_COLUMN} = false");

        // Assert
        Assert.IsTrue(result.Success, $"Failed to get users where not active = false: {result.Error}");
        Assert.IsNull(result.Error);

        // Verify the returned data
        Assert.IsNotNull(result.Data);
        var resultRows = result.Data as IList<Row>;
        Assert.IsNotNull(resultRows);

        Assert.AreEqual(3, resultRows!.Count);

        // Verify all returned users are active
        foreach (var row in resultRows)
        {
            Assert.IsTrue((bool)row.Fields[ACTIVE_COLUMN]!);
        }

        // Check specifically for expected users
        var names = resultRows.Select(r => r.Fields[NAME_COLUMN]?.ToString()).ToList();
        Assert.IsTrue(names.Contains(JOHN_NAME));
        Assert.IsTrue(names.Contains(JANE_NAME));
        Assert.IsTrue(names.Contains(BOB_NAME));
        Assert.IsFalse(names.Contains(ALICE_NAME));
    }

    [TestMethod]
    public void Test_GetUsers_WhereNameIn_Works()
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

        // Insert test users
        var johnResult = _connection.Execute($"upsert {USERS_TABLE} {{ {NAME_COLUMN}: '{JOHN_NAME}', {AGE_COLUMN}: {JOHN_AGE}, {ACTIVE_COLUMN}: {JOHN_ACTIVE.ToString().ToLower()} }}");
        Assert.IsTrue(johnResult.Success);
        var janeResult = _connection.Execute($"upsert {USERS_TABLE} {{ {NAME_COLUMN}: '{JANE_NAME}', {AGE_COLUMN}: {JANE_AGE}, {ACTIVE_COLUMN}: {JANE_ACTIVE.ToString().ToLower()} }}");
        Assert.IsTrue(janeResult.Success);
        var aliceResult = _connection.Execute($"upsert {USERS_TABLE} {{ {NAME_COLUMN}: '{ALICE_NAME}', {AGE_COLUMN}: {ALICE_AGE}, {ACTIVE_COLUMN}: {ALICE_ACTIVE.ToString().ToLower()} }}");
        Assert.IsTrue(aliceResult.Success);
        var bobResult = _connection.Execute($"upsert {USERS_TABLE} {{ {NAME_COLUMN}: '{BOB_NAME}', {AGE_COLUMN}: {BOB_AGE}, {ACTIVE_COLUMN}: {BOB_ACTIVE.ToString().ToLower()} }}");
        Assert.IsTrue(bobResult.Success);

        // Act
        var result = _connection.Execute($"get {USERS_TABLE} where {NAME_COLUMN} in ['{JOHN_NAME}', '{JANE_NAME}', '{BOB_NAME}']");

        // Assert
        Assert.IsTrue(result.Success, $"Failed to get users with name in list: {result.Error}");
        Assert.IsNull(result.Error);

        // Verify the returned data
        Assert.IsNotNull(result.Data);
        var resultRows = result.Data as IList<Row>;
        Assert.IsNotNull(resultRows);

        // Should return John, Jane, and Bob, but not Alice
        Assert.AreEqual(3, resultRows!.Count);

        // Check specifically for expected users
        var names = resultRows.Select(r => r.Fields[NAME_COLUMN]?.ToString()).ToList();
        Assert.IsTrue(names.Contains(JOHN_NAME));
        Assert.IsTrue(names.Contains(JANE_NAME));
        Assert.IsTrue(names.Contains(BOB_NAME));
        Assert.IsFalse(names.Contains(ALICE_NAME));
    }

    [TestMethod]
    public void Test_GetUsers_WhereNameContains_Works()
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

        // Insert test users
        var johnResult = _connection.Execute($"upsert {USERS_TABLE} {{ {NAME_COLUMN}: '{JOHN_NAME}', {AGE_COLUMN}: {JOHN_AGE}, {ACTIVE_COLUMN}: {JOHN_ACTIVE.ToString().ToLower()} }}");
        Assert.IsTrue(johnResult.Success);
        var janeResult = _connection.Execute($"upsert {USERS_TABLE} {{ {NAME_COLUMN}: '{JANE_NAME}', {AGE_COLUMN}: {JANE_AGE}, {ACTIVE_COLUMN}: {JANE_ACTIVE.ToString().ToLower()} }}");
        Assert.IsTrue(janeResult.Success);
        var aliceResult = _connection.Execute($"upsert {USERS_TABLE} {{ {NAME_COLUMN}: '{ALICE_NAME}', {AGE_COLUMN}: {ALICE_AGE}, {ACTIVE_COLUMN}: {ALICE_ACTIVE.ToString().ToLower()} }}");
        Assert.IsTrue(aliceResult.Success);
        var bobResult = _connection.Execute($"upsert {USERS_TABLE} {{ {NAME_COLUMN}: '{BOB_NAME}', {AGE_COLUMN}: {BOB_AGE}, {ACTIVE_COLUMN}: {BOB_ACTIVE.ToString().ToLower()} }}");
        Assert.IsTrue(bobResult.Success);

        // Act
        var result = _connection.Execute($"get {USERS_TABLE} where {NAME_COLUMN} contains 'oh'");

        // Assert
        Assert.IsTrue(result.Success, $"Failed to get users with name containing 'oh': {result.Error}");
        Assert.IsNull(result.Error);

        // Verify the returned data
        Assert.IsNotNull(result.Data);
        var resultRows = result.Data as IList<Row>;
        Assert.IsNotNull(resultRows);

        Assert.AreEqual(2, resultRows!.Count);

        // Check specifically for expected users
        var names = resultRows.Select(r => r.Fields[NAME_COLUMN]?.ToString()).ToList();
        Assert.IsTrue(names.Contains(JOHN_NAME));
        Assert.IsTrue(names.Contains(BOB_NAME));
        Assert.IsFalse(names.Contains(JANE_NAME));
        Assert.IsFalse(names.Contains(ALICE_NAME));
    }


}
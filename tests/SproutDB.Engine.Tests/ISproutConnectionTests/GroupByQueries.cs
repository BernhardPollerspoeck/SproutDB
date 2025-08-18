using SproutDB.Engine.Core;

namespace SproutDB.Engine.Tests.ISproutConnectionTests;

[TestClass]
public class GroupByQueries : ISproutConnectionTestsSetup
{
    [TestMethod]
    public void Test_GetUsers_GroupByActive_Works()
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
        var aliceResult = _connection.Execute($"upsert {USERS_TABLE} {{ {NAME_COLUMN}: '{ALICE_NAME}', {AGE_COLUMN}: {ALICE_AGE}, {ACTIVE_COLUMN}: false }}");
        Assert.IsTrue(aliceResult.Success);
        var bobResult = _connection.Execute($"upsert {USERS_TABLE} {{ {NAME_COLUMN}: '{BOB_NAME}', {AGE_COLUMN}: {BOB_AGE}, {ACTIVE_COLUMN}: false }}");
        Assert.IsTrue(bobResult.Success);

        // Act
        var result = _connection.Execute($"get {USERS_TABLE} group by {ACTIVE_COLUMN}");

        // Assert
        Assert.IsTrue(result.Success, $"Failed to group users by active: {result.Error}");
        Assert.IsNull(result.Error);

        // Verify the returned data
        Assert.IsNotNull(result.Data);
        var resultRows = result.Data as IList<Row>;
        Assert.IsNotNull(resultRows);

        // Should return 2 groups: active=true and active=false
        Assert.AreEqual(2, resultRows!.Count);

        // Verify that the two groups are distinct
        var activeValues = resultRows.Select(r => r.Fields[ACTIVE_COLUMN]).ToList();
        Assert.IsTrue(activeValues.Contains(true));
        Assert.IsTrue(activeValues.Contains(false));

        // By default, the group by should include the grouped field
        Assert.IsTrue(resultRows.All(r => r.Fields.ContainsKey(ACTIVE_COLUMN)));
    }

    [TestMethod]
    public void Test_GetUsers_GroupByActive_SelectActiveAndCount_Works()
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
        var aliceResult = _connection.Execute($"upsert {USERS_TABLE} {{ {NAME_COLUMN}: '{ALICE_NAME}', {AGE_COLUMN}: {ALICE_AGE}, {ACTIVE_COLUMN}: false }}");
        Assert.IsTrue(aliceResult.Success);
        var bobResult = _connection.Execute($"upsert {USERS_TABLE} {{ {NAME_COLUMN}: '{BOB_NAME}', {AGE_COLUMN}: {BOB_AGE}, {ACTIVE_COLUMN}: false }}");
        Assert.IsTrue(bobResult.Success);

        // Act
        var result = _connection.Execute($"get {USERS_TABLE} group by {ACTIVE_COLUMN} select {ACTIVE_COLUMN}, count() as count");

        // Assert
        Assert.IsTrue(result.Success, $"Failed to group users by active and count: {result.Error}");
        Assert.IsNull(result.Error);

        // Verify the returned data
        Assert.IsNotNull(result.Data);
        var resultRows = result.Data as IList<Row>;
        Assert.IsNotNull(resultRows);

        // Should return 2 groups: active=true and active=false
        Assert.AreEqual(2, resultRows!.Count);

        // Verify the contents of each group
        foreach (var row in resultRows)
        {
            // Each row should have the active field and count field
            Assert.IsTrue(row.Fields.ContainsKey(ACTIVE_COLUMN));
            Assert.IsTrue(row.Fields.ContainsKey("count"));

            // Verify count is correct for each group
            if ((bool)row.Fields[ACTIVE_COLUMN] == true)
            {
                Assert.AreEqual(2, row.Fields["count"]); // 2 active users
            }
            else
            {
                Assert.AreEqual(2, row.Fields["count"]); // 2 inactive users
            }
        }
    }

    [TestMethod]
    public void Test_GetUsers_GroupByActive_HavingCountGreaterThan2_Works()
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

        // Insert test users with different active states - 3 active and 2 inactive
        var johnResult = _connection.Execute($"upsert {USERS_TABLE} {{ {NAME_COLUMN}: '{JOHN_NAME}', {AGE_COLUMN}: {JOHN_AGE}, {ACTIVE_COLUMN}: true }}");
        Assert.IsTrue(johnResult.Success);
        var janeResult = _connection.Execute($"upsert {USERS_TABLE} {{ {NAME_COLUMN}: '{JANE_NAME}', {AGE_COLUMN}: {JANE_AGE}, {ACTIVE_COLUMN}: true }}");
        Assert.IsTrue(janeResult.Success);
        var charlesResult = _connection.Execute($"upsert {USERS_TABLE} {{ {NAME_COLUMN}: 'Charles Brown', {AGE_COLUMN}: 45, {ACTIVE_COLUMN}: true }}");
        Assert.IsTrue(charlesResult.Success);
        var aliceResult = _connection.Execute($"upsert {USERS_TABLE} {{ {NAME_COLUMN}: '{ALICE_NAME}', {AGE_COLUMN}: {ALICE_AGE}, {ACTIVE_COLUMN}: false }}");
        Assert.IsTrue(aliceResult.Success);
        var bobResult = _connection.Execute($"upsert {USERS_TABLE} {{ {NAME_COLUMN}: '{BOB_NAME}', {AGE_COLUMN}: {BOB_AGE}, {ACTIVE_COLUMN}: false }}");
        Assert.IsTrue(bobResult.Success);

        // Act
        var result = _connection.Execute($"get {USERS_TABLE} group by {ACTIVE_COLUMN} having count() > 2");

        // Assert
        Assert.IsTrue(result.Success, $"Failed to group users with having clause: {result.Error}");
        Assert.IsNull(result.Error);

        // Verify the returned data
        Assert.IsNotNull(result.Data);
        var resultRows = result.Data as IList<Row>;
        Assert.IsNotNull(resultRows);

        // Should return only 1 group: active=true (which has 3 users)
        Assert.AreEqual(1, resultRows!.Count);
        Assert.AreEqual(true, resultRows[0].Fields[ACTIVE_COLUMN]);
    }

    [TestMethod]
    public void Test_GetOrders_GroupByUserId_SelectUserIdAndSumTotal_Works()
    {
        // Arrange
        var createDbResult = _connection.Execute($"create database {TEST_DATABASE}");
        Assert.IsTrue(createDbResult.Success);
        
        // Create orders table
        var createTableResult = _connection.Execute($"create table {ORDERS_TABLE}");
        Assert.IsTrue(createTableResult.Success);
        var addUserIdColumnResult = _connection.Execute($"add column {ORDERS_TABLE}.user_id {NUMBER_TYPE}");
        Assert.IsTrue(addUserIdColumnResult.Success);
        var addTotalColumnResult = _connection.Execute($"add column {ORDERS_TABLE}.{TOTAL_COLUMN} {NUMBER_TYPE}");
        Assert.IsTrue(addTotalColumnResult.Success);

        // Insert test orders for different users
        // User 1 has 2 orders
        var order1Result = _connection.Execute($"upsert {ORDERS_TABLE} {{ user_id: 1, {TOTAL_COLUMN}: 100.50 }}");
        Assert.IsTrue(order1Result.Success);
        var order2Result = _connection.Execute($"upsert {ORDERS_TABLE} {{ user_id: 1, {TOTAL_COLUMN}: 200.75 }}");
        Assert.IsTrue(order2Result.Success);
        
        // User 2 has 3 orders
        var order3Result = _connection.Execute($"upsert {ORDERS_TABLE} {{ user_id: 2, {TOTAL_COLUMN}: 50.25 }}");
        Assert.IsTrue(order3Result.Success);
        var order4Result = _connection.Execute($"upsert {ORDERS_TABLE} {{ user_id: 2, {TOTAL_COLUMN}: 75.50 }}");
        Assert.IsTrue(order4Result.Success);
        var order5Result = _connection.Execute($"upsert {ORDERS_TABLE} {{ user_id: 2, {TOTAL_COLUMN}: 125.00 }}");
        Assert.IsTrue(order5Result.Success);

        // Calculate expected totals for each user
        var user1ExpectedTotal = 100.50 + 200.75;
        var user2ExpectedTotal = 50.25 + 75.50 + 125.00;

        // Act
        var result = _connection.Execute($"get {ORDERS_TABLE} group by user_id select user_id, sum({TOTAL_COLUMN}) as total_spent");

        // Assert
        Assert.IsTrue(result.Success, $"Failed to group orders by user_id and sum totals: {result.Error}");
        Assert.IsNull(result.Error);

        // Verify the returned data
        Assert.IsNotNull(result.Data);
        var resultRows = result.Data as IList<Row>;
        Assert.IsNotNull(resultRows);

        // Should return 2 groups: user_id=1 and user_id=2
        Assert.AreEqual(2, resultRows!.Count);

        // Verify the contents of each group
        foreach (var row in resultRows)
        {
            // Each row should have the user_id field and total_spent field
            Assert.IsTrue(row.Fields.ContainsKey("user_id"));
            Assert.IsTrue(row.Fields.ContainsKey("total_spent"));

            // Verify sum is correct for each user
            if ((double)row.Fields["user_id"] == 1)
            {
                Assert.AreEqual(user1ExpectedTotal, (double)row.Fields["total_spent"], 0.001);
            }
            else if ((double)row.Fields["user_id"] == 2)
            {
                Assert.AreEqual(user2ExpectedTotal, (double)row.Fields["total_spent"], 0.001);
            }
        }
    }

    [TestMethod]
    public void Test_GroupBy_WithEmptyResult_Works()
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

        // No data inserted - empty table

        // Act
        var result = _connection.Execute($"get {USERS_TABLE} group by {ACTIVE_COLUMN} select {ACTIVE_COLUMN}, count() as count");

        // Assert
        Assert.IsTrue(result.Success, $"Failed to group on empty table: {result.Error}");
        Assert.IsNull(result.Error);

        // Verify the returned data
        Assert.IsNotNull(result.Data);
        var resultRows = result.Data as IList<Row>;
        Assert.IsNotNull(resultRows);

        // Should return empty result set
        Assert.AreEqual(0, resultRows!.Count);
    }

    [TestMethod]
    public void Test_GroupBy_WithNullValues_Works()
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
        var addCityColumnResult = _connection.Execute($"add column {USERS_TABLE}.city {STRING_TYPE}");
        Assert.IsTrue(addCityColumnResult.Success);

        // Insert test users with some null cities
        var johnResult = _connection.Execute($"upsert {USERS_TABLE} {{ {NAME_COLUMN}: '{JOHN_NAME}', {AGE_COLUMN}: {JOHN_AGE}, city: 'New York' }}");
        Assert.IsTrue(johnResult.Success);
        var janeResult = _connection.Execute($"upsert {USERS_TABLE} {{ {NAME_COLUMN}: '{JANE_NAME}', {AGE_COLUMN}: {JANE_AGE}, city: 'New York' }}");
        Assert.IsTrue(janeResult.Success);
        var aliceResult = _connection.Execute($"upsert {USERS_TABLE} {{ {NAME_COLUMN}: '{ALICE_NAME}', {AGE_COLUMN}: {ALICE_AGE}, city: 'Boston' }}");
        Assert.IsTrue(aliceResult.Success);
        var bobResult = _connection.Execute($"upsert {USERS_TABLE} {{ {NAME_COLUMN}: '{BOB_NAME}', {AGE_COLUMN}: {BOB_AGE}, city: null }}");
        Assert.IsTrue(bobResult.Success);
        var charlesResult = _connection.Execute($"upsert {USERS_TABLE} {{ {NAME_COLUMN}: 'Charles Brown', {AGE_COLUMN}: 45, city: null }}");
        Assert.IsTrue(charlesResult.Success);

        // Act
        var result = _connection.Execute($"get {USERS_TABLE} group by city select city, count() as count");

        // Assert
        Assert.IsTrue(result.Success, $"Failed to group with null values: {result.Error}");
        Assert.IsNull(result.Error);

        // Verify the returned data
        Assert.IsNotNull(result.Data);
        var resultRows = result.Data as IList<Row>;
        Assert.IsNotNull(resultRows);

        // Should return 3 groups: New York, Boston, and null
        Assert.AreEqual(3, resultRows!.Count);

        // Verify the contents of each group
        var newYorkRow = resultRows.FirstOrDefault(r => r.Fields["city"]?.ToString() == "New York");
        var bostonRow = resultRows.FirstOrDefault(r => r.Fields["city"]?.ToString() == "Boston");
        var nullRow = resultRows.FirstOrDefault(r => r.Fields["city"] == null);

        Assert.IsNotNull(newYorkRow);
        Assert.IsNotNull(bostonRow);
        Assert.IsNotNull(nullRow);

        Assert.AreEqual(2, newYorkRow!.Fields["count"]); // 2 users in New York
        Assert.AreEqual(1, bostonRow!.Fields["count"]); // 1 user in Boston
        Assert.AreEqual(2, nullRow!.Fields["count"]); // 2 users with null city
    }
}
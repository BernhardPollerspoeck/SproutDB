using SproutDB.Engine.Core;

namespace SproutDB.Engine.Tests.ISproutConnectionTests;

[TestClass]
public class ComplexQueries : ISproutConnectionTestsSetup
{
    [TestMethod]
    public void Test_GetUsers_WithComplexJoinGroupByHavingOrderByPagination_Works()
    {
        // Arrange
        var createDbResult = _connection.Execute($"create database {TEST_DATABASE}");
        Assert.IsTrue(createDbResult.Success);

        // Create users table
        var createUsersTableResult = _connection.Execute($"create table {USERS_TABLE}");
        Assert.IsTrue(createUsersTableResult.Success);
        var addUserIdColumnResult = _connection.Execute($"add column {USERS_TABLE}.id {NUMBER_TYPE}");
        Assert.IsTrue(addUserIdColumnResult.Success);
        var addNameColumnResult = _connection.Execute($"add column {USERS_TABLE}.{NAME_COLUMN} {STRING_TYPE}");
        Assert.IsTrue(addNameColumnResult.Success);

        // Create orders table
        var createOrdersTableResult = _connection.Execute($"create table {ORDERS_TABLE}");
        Assert.IsTrue(createOrdersTableResult.Success);
        var addOrderIdColumnResult = _connection.Execute($"add column {ORDERS_TABLE}.id {NUMBER_TYPE}");
        Assert.IsTrue(addOrderIdColumnResult.Success);
        var addUserIdFkColumnResult = _connection.Execute($"add column {ORDERS_TABLE}.user_id {NUMBER_TYPE}");
        Assert.IsTrue(addUserIdFkColumnResult.Success);
        var addTotalColumnResult = _connection.Execute($"add column {ORDERS_TABLE}.{TOTAL_COLUMN} {NUMBER_TYPE}");
        Assert.IsTrue(addTotalColumnResult.Success);

        // Insert users with various names
        var users = new[]
        {
            (Name: "John Smith", Id: 1),
            (Name: "Jane Smith", Id: 2),
            (Name: "Alice Johnson", Id: 3),
            (Name: "Bob Williams", Id: 4),
            (Name: "Carol Davis", Id: 5),
            (Name: "David Wilson", Id: 6),
            (Name: "Emma Brown", Id: 7),
            (Name: "Frank Jones", Id: 8),
            (Name: "Grace Miller", Id: 9),
            (Name: "Henry Taylor", Id: 10),
            (Name: "Ivy Anderson", Id: 11),
            (Name: "Jack Thomas", Id: 12),
        };

        foreach (var user in users)
        {
            var userResult = _connection.Execute($"upsert {USERS_TABLE} {{ id: {user.Id}, {NAME_COLUMN}: '{user.Name}' }}");
            Assert.IsTrue(userResult.Success);
        }

        // Insert orders - create different patterns of orders per user
        // Users with more than 2 orders > 100:
        // - John Smith: 3 orders > 100
        // - Jane Smith: 4 orders > 100
        // - Bob Williams: 3 orders > 100
        // - Frank Jones: 5 orders > 100
        
        // Users with 2 or fewer orders > 100:
        // - Alice Johnson: 2 orders > 100
        // - Carol Davis: 1 order > 100
        // - David Wilson: 2 orders > 100
        // - Emma Brown: 0 orders > 100 (but 1 order <= 100)
        // - Grace Miller: 2 orders > 100
        
        // Users with no orders:
        // - Henry Taylor
        // - Ivy Anderson
        // - Jack Thomas

        // Order ID counter
        int orderId = 1;

        // John Smith - 3 orders > 100
        _connection.Execute($"upsert {ORDERS_TABLE} {{ id: {orderId++}, user_id: 1, {TOTAL_COLUMN}: 150.00 }}");
        _connection.Execute($"upsert {ORDERS_TABLE} {{ id: {orderId++}, user_id: 1, {TOTAL_COLUMN}: 200.00 }}");
        _connection.Execute($"upsert {ORDERS_TABLE} {{ id: {orderId++}, user_id: 1, {TOTAL_COLUMN}: 120.00 }}");
        _connection.Execute($"upsert {ORDERS_TABLE} {{ id: {orderId++}, user_id: 1, {TOTAL_COLUMN}: 50.00 }}"); // <= 100, shouldn't count

        // Jane Smith - 4 orders > 100
        _connection.Execute($"upsert {ORDERS_TABLE} {{ id: {orderId++}, user_id: 2, {TOTAL_COLUMN}: 180.00 }}");
        _connection.Execute($"upsert {ORDERS_TABLE} {{ id: {orderId++}, user_id: 2, {TOTAL_COLUMN}: 220.00 }}");
        _connection.Execute($"upsert {ORDERS_TABLE} {{ id: {orderId++}, user_id: 2, {TOTAL_COLUMN}: 150.00 }}");
        _connection.Execute($"upsert {ORDERS_TABLE} {{ id: {orderId++}, user_id: 2, {TOTAL_COLUMN}: 190.00 }}");

        // Alice Johnson - 2 orders > 100
        _connection.Execute($"upsert {ORDERS_TABLE} {{ id: {orderId++}, user_id: 3, {TOTAL_COLUMN}: 110.00 }}");
        _connection.Execute($"upsert {ORDERS_TABLE} {{ id: {orderId++}, user_id: 3, {TOTAL_COLUMN}: 120.00 }}");

        // Bob Williams - 3 orders > 100
        _connection.Execute($"upsert {ORDERS_TABLE} {{ id: {orderId++}, user_id: 4, {TOTAL_COLUMN}: 120.00 }}");
        _connection.Execute($"upsert {ORDERS_TABLE} {{ id: {orderId++}, user_id: 4, {TOTAL_COLUMN}: 130.00 }}");
        _connection.Execute($"upsert {ORDERS_TABLE} {{ id: {orderId++}, user_id: 4, {TOTAL_COLUMN}: 140.00 }}");

        // Carol Davis - 1 order > 100
        _connection.Execute($"upsert {ORDERS_TABLE} {{ id: {orderId++}, user_id: 5, {TOTAL_COLUMN}: 200.00 }}");

        // David Wilson - 2 orders > 100
        _connection.Execute($"upsert {ORDERS_TABLE} {{ id: {orderId++}, user_id: 6, {TOTAL_COLUMN}: 130.00 }}");
        _connection.Execute($"upsert {ORDERS_TABLE} {{ id: {orderId++}, user_id: 6, {TOTAL_COLUMN}: 170.00 }}");

        // Emma Brown - 0 orders > 100
        _connection.Execute($"upsert {ORDERS_TABLE} {{ id: {orderId++}, user_id: 7, {TOTAL_COLUMN}: 90.00 }}");

        // Frank Jones - 5 orders > 100
        _connection.Execute($"upsert {ORDERS_TABLE} {{ id: {orderId++}, user_id: 8, {TOTAL_COLUMN}: 110.00 }}");
        _connection.Execute($"upsert {ORDERS_TABLE} {{ id: {orderId++}, user_id: 8, {TOTAL_COLUMN}: 120.00 }}");
        _connection.Execute($"upsert {ORDERS_TABLE} {{ id: {orderId++}, user_id: 8, {TOTAL_COLUMN}: 130.00 }}");
        _connection.Execute($"upsert {ORDERS_TABLE} {{ id: {orderId++}, user_id: 8, {TOTAL_COLUMN}: 140.00 }}");
        _connection.Execute($"upsert {ORDERS_TABLE} {{ id: {orderId++}, user_id: 8, {TOTAL_COLUMN}: 150.00 }}");

        // Grace Miller - 2 orders > 100
        _connection.Execute($"upsert {ORDERS_TABLE} {{ id: {orderId++}, user_id: 9, {TOTAL_COLUMN}: 105.00 }}");
        _connection.Execute($"upsert {ORDERS_TABLE} {{ id: {orderId++}, user_id: 9, {TOTAL_COLUMN}: 115.00 }}");

        // Henry, Ivy, and Jack have no orders

        // Act - complex query with join, where, group by, having, order by, select with aggregates, and pagination
        var query = $@"get {USERS_TABLE} 
                      follow {USERS_TABLE}.id -> {ORDERS_TABLE}.user_id as orders 
                      where orders.{TOTAL_COLUMN} > 100 
                      group by {USERS_TABLE}.{NAME_COLUMN} 
                      having count(orders.id) > 2 
                      order by count(orders.id) desc 
                      select {USERS_TABLE}.{NAME_COLUMN}, count(orders.id) as order_count, sum(orders.{TOTAL_COLUMN}) as total_spent
                      page 1 of size 10";

        var result = _connection.Execute(query);

        // Assert
        Assert.IsTrue(result.Success, $"Failed to execute complex query: {result.Error}");
        Assert.IsNull(result.Error);

        // Verify the returned data
        Assert.IsNotNull(result.Data);
        var resultRows = result.Data as IList<Row>;
        Assert.IsNotNull(resultRows);

        // Should return 4 users with more than 2 orders > 100
        Assert.AreEqual(4, resultRows!.Count);

        // Expected order by count(orders) desc:
        // 1. Frank Jones (5 orders)
        // 2. Jane Smith (4 orders)
        // 3. John Smith (3 orders)
        // 4. Bob Williams (3 orders)

        // Verify first row (Frank Jones)
        Assert.AreEqual("Frank Jones", resultRows[0].Fields[$"{USERS_TABLE}.{NAME_COLUMN}"]);
        Assert.AreEqual(5, resultRows[0].Fields["order_count"]);
        Assert.AreEqual(650.0, (double)resultRows[0].Fields["total_spent"], 0.001);

        // Verify second row (Jane Smith)
        Assert.AreEqual("Jane Smith", resultRows[1].Fields[$"{USERS_TABLE}.{NAME_COLUMN}"]);
        Assert.AreEqual(4, resultRows[1].Fields["order_count"]);
        Assert.AreEqual(740.0, (double)resultRows[1].Fields["total_spent"], 0.001);

        // Verify that all rows have order_count > 2
        foreach (var row in resultRows)
        {
            Assert.IsTrue((int)row.Fields["order_count"] > 2);
        }
    }
    
    [TestMethod]
    public void Test_GetUsers_ComplexQueryWithAdditionalPage_Works()
    {
        // This test verifies that pagination works correctly with the complex query
        // We'll add more users and orders and test page 2
        
        // Arrange
        var createDbResult = _connection.Execute($"create database {TEST_DATABASE}");
        Assert.IsTrue(createDbResult.Success);

        // Create users table
        var createUsersTableResult = _connection.Execute($"create table {USERS_TABLE}");
        Assert.IsTrue(createUsersTableResult.Success);
        var addUserIdColumnResult = _connection.Execute($"add column {USERS_TABLE}.id {NUMBER_TYPE}");
        Assert.IsTrue(addUserIdColumnResult.Success);
        var addNameColumnResult = _connection.Execute($"add column {USERS_TABLE}.{NAME_COLUMN} {STRING_TYPE}");
        Assert.IsTrue(addNameColumnResult.Success);

        // Create orders table
        var createOrdersTableResult = _connection.Execute($"create table {ORDERS_TABLE}");
        Assert.IsTrue(createOrdersTableResult.Success);
        var addOrderIdColumnResult = _connection.Execute($"add column {ORDERS_TABLE}.id {NUMBER_TYPE}");
        Assert.IsTrue(addOrderIdColumnResult.Success);
        var addUserIdFkColumnResult = _connection.Execute($"add column {ORDERS_TABLE}.user_id {NUMBER_TYPE}");
        Assert.IsTrue(addUserIdFkColumnResult.Success);
        var addTotalColumnResult = _connection.Execute($"add column {ORDERS_TABLE}.{TOTAL_COLUMN} {NUMBER_TYPE}");
        Assert.IsTrue(addTotalColumnResult.Success);

        // Create 15 users with varying numbers of orders > 100
        for (int i = 1; i <= 15; i++)
        {
            var userResult = _connection.Execute($"upsert {USERS_TABLE} {{ id: {i}, {NAME_COLUMN}: 'User {i}' }}");
            Assert.IsTrue(userResult.Success);
            
            // Each user i will have i orders > 100
            // This ensures users 1 and 2 have < 3 orders (won't match having clause)
            // Users 3-15 will have 3+ orders and match the having clause
            for (int j = 1; j <= i && j <= 10; j++) // Cap at 10 orders per user
            {
                var orderResult = _connection.Execute($"upsert {ORDERS_TABLE} {{ id: {i*100 + j}, user_id: {i}, {TOTAL_COLUMN}: {100.0 + j*10} }}");
                Assert.IsTrue(orderResult.Success);
            }
        }

        // Act - Get page 2 with page size 5
        var query = $@"get {USERS_TABLE} 
                      follow {USERS_TABLE}.id -> {ORDERS_TABLE}.user_id as orders 
                      where orders.{TOTAL_COLUMN} > 100 
                      group by {USERS_TABLE}.{NAME_COLUMN} 
                      having count(orders.id) > 2 
                      order by count(orders.id) desc 
                      select {USERS_TABLE}.{NAME_COLUMN}, count(orders.id) as order_count, sum(orders.{TOTAL_COLUMN}) as total_spent
                      page 2 of size 5";

        var result = _connection.Execute(query);

        // Assert
        Assert.IsTrue(result.Success, $"Failed to execute complex query with pagination: {result.Error}");
        Assert.IsNull(result.Error);

        // Verify the returned data
        Assert.IsNotNull(result.Data);
        var resultRows = result.Data as IList<Row>;
        Assert.IsNotNull(resultRows);

        // Should return 5 users (the second page)
        Assert.AreEqual(5, resultRows!.Count);

        // First page would have users with 10, 9, 8, 7, 6 orders
        // Second page should start with users with 5 orders
        Assert.AreEqual("User 15", resultRows[0].Fields[$"{USERS_TABLE}.{NAME_COLUMN}"]);
        Assert.AreEqual(10, resultRows[0].Fields["order_count"]);

        // Verify that all rows have order_count > 2
        foreach (var row in resultRows)
        {
            Assert.IsTrue((int)row.Fields["order_count"] > 2);
        }
        
        // Check that order counts are in descending order
        for (int i = 1; i < resultRows.Count; i++)
        {
            Assert.IsTrue((int)resultRows[i-1].Fields["order_count"] >= (int)resultRows[i].Fields["order_count"]);
        }
    }

    [TestMethod]
    public void Test_GetUsers_ComplexQueryWithNoResults_Works()
    {
        // This test verifies that the complex query works correctly when no results match
        
        // Arrange
        var createDbResult = _connection.Execute($"create database {TEST_DATABASE}");
        Assert.IsTrue(createDbResult.Success);

        // Create users table
        var createUsersTableResult = _connection.Execute($"create table {USERS_TABLE}");
        Assert.IsTrue(createUsersTableResult.Success);
        var addUserIdColumnResult = _connection.Execute($"add column {USERS_TABLE}.id {NUMBER_TYPE}");
        Assert.IsTrue(addUserIdColumnResult.Success);
        var addNameColumnResult = _connection.Execute($"add column {USERS_TABLE}.{NAME_COLUMN} {STRING_TYPE}");
        Assert.IsTrue(addNameColumnResult.Success);

        // Create orders table
        var createOrdersTableResult = _connection.Execute($"create table {ORDERS_TABLE}");
        Assert.IsTrue(createOrdersTableResult.Success);
        var addOrderIdColumnResult = _connection.Execute($"add column {ORDERS_TABLE}.id {NUMBER_TYPE}");
        Assert.IsTrue(addOrderIdColumnResult.Success);
        var addUserIdFkColumnResult = _connection.Execute($"add column {ORDERS_TABLE}.user_id {NUMBER_TYPE}");
        Assert.IsTrue(addUserIdFkColumnResult.Success);
        var addTotalColumnResult = _connection.Execute($"add column {ORDERS_TABLE}.{TOTAL_COLUMN} {NUMBER_TYPE}");
        Assert.IsTrue(addTotalColumnResult.Success);

        // Create users with at most 2 orders > 100
        for (int i = 1; i <= 5; i++)
        {
            var userResult = _connection.Execute($"upsert {USERS_TABLE} {{ id: {i}, {NAME_COLUMN}: 'User {i}' }}");
            Assert.IsTrue(userResult.Success);
            
            // Each user will have at most 2 orders > 100 (won't satisfy having count > 2)
            for (int j = 1; j <= 2; j++)
            {
                var orderResult = _connection.Execute($"upsert {ORDERS_TABLE} {{ id: {i*100 + j}, user_id: {i}, {TOTAL_COLUMN}: {100.0 + j*10} }}");
                Assert.IsTrue(orderResult.Success);
            }
        }

        // Act - Execute the complex query
        var query = $@"get {USERS_TABLE} 
                      follow {USERS_TABLE}.id -> {ORDERS_TABLE}.user_id as orders 
                      where orders.{TOTAL_COLUMN} > 100 
                      group by {USERS_TABLE}.{NAME_COLUMN} 
                      having count(orders.id) > 2 
                      order by count(orders.id) desc 
                      select {USERS_TABLE}.{NAME_COLUMN}, count(orders.id) as order_count, sum(orders.{TOTAL_COLUMN}) as total_spent
                      page 1 of size 10";

        var result = _connection.Execute(query);

        // Assert
        Assert.IsTrue(result.Success, $"Failed to execute complex query with no matching results: {result.Error}");
        Assert.IsNull(result.Error);

        // Verify the returned data is empty
        Assert.IsNotNull(result.Data);
        var resultRows = result.Data as IList<Row>;
        Assert.IsNotNull(resultRows);

        // Should return 0 users (none satisfy having count > 2)
        Assert.AreEqual(0, resultRows!.Count);
    }
}
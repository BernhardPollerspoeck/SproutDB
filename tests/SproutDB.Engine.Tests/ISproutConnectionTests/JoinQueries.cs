using SproutDB.Engine.Core;

namespace SproutDB.Engine.Tests.ISproutConnectionTests;

[TestClass]
public class JoinQueries : ISproutConnectionTestsSetup
{
    [TestMethod]
    public void Test_GetUsers_FollowOrders_Works()
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
        
        // Insert test users
        var johnResult = _connection.Execute($"upsert {USERS_TABLE} {{ id: 1, {NAME_COLUMN}: '{JOHN_NAME}' }}");
        Assert.IsTrue(johnResult.Success);
        var janeResult = _connection.Execute($"upsert {USERS_TABLE} {{ id: 2, {NAME_COLUMN}: '{JANE_NAME}' }}");
        Assert.IsTrue(janeResult.Success);
        var aliceResult = _connection.Execute($"upsert {USERS_TABLE} {{ id: 3, {NAME_COLUMN}: '{ALICE_NAME}' }}");
        Assert.IsTrue(aliceResult.Success);
        
        // Insert test orders
        var order1Result = _connection.Execute($"upsert {ORDERS_TABLE} {{ id: 101, user_id: 1, {TOTAL_COLUMN}: 150.50 }}");
        Assert.IsTrue(order1Result.Success);
        var order2Result = _connection.Execute($"upsert {ORDERS_TABLE} {{ id: 102, user_id: 1, {TOTAL_COLUMN}: 200.75 }}");
        Assert.IsTrue(order2Result.Success);
        var order3Result = _connection.Execute($"upsert {ORDERS_TABLE} {{ id: 103, user_id: 2, {TOTAL_COLUMN}: 75.25 }}");
        Assert.IsTrue(order3Result.Success);
        // Note: Alice (id: 3) has no orders
        
        // Act
        var result = _connection.Execute($"get {USERS_TABLE} follow {USERS_TABLE}.id -> {ORDERS_TABLE}.user_id as ordars");
        
        // Assert
        Assert.IsTrue(result.Success, $"Failed to join users with orders: {result.Error}");
        Assert.IsNull(result.Error);
        
        // Verify the returned data
        Assert.IsNotNull(result.Data);
        var resultRows = result.Data as IList<Row>;
        Assert.IsNotNull(resultRows);
        
        // Should return one row for each joined pair (only matched records in inner join)
        Assert.AreEqual(3, resultRows!.Count); // 2 orders for John, 1 for Jane, 0 for Alice (inner join by default)
        
        // Verify user and order fields are present
        Assert.IsTrue(resultRows.All(r => r.Fields.ContainsKey("users.id")));
        Assert.IsTrue(resultRows.All(r => r.Fields.ContainsKey($"users.{NAME_COLUMN}")));
        Assert.IsTrue(resultRows.All(r => r.Fields.ContainsKey("ordars.id")));
        Assert.IsTrue(resultRows.All(r => r.Fields.ContainsKey("ordars.user_id")));
        Assert.IsTrue(resultRows.All(r => r.Fields.ContainsKey($"ordars.{TOTAL_COLUMN}")));
        
        // Verify the correct relationships
        var johnOrders = resultRows.Where(r => (int)r.Fields["users.id"] == 1).ToList();
        var janeOrders = resultRows.Where(r => (int)r.Fields["users.id"] == 2).ToList();
        
        Assert.AreEqual(2, johnOrders.Count);
        Assert.AreEqual(1, janeOrders.Count);
        
        // Verify order totals for John
        var johnOrderTotals = johnOrders.Select(r => (double)r.Fields[$"ordars.{TOTAL_COLUMN}"]).ToList();
        Assert.IsTrue(johnOrderTotals.Contains(150.50));
        Assert.IsTrue(johnOrderTotals.Contains(200.75));
        
        // Verify order total for Jane
        Assert.AreEqual(75.25, (double)janeOrders[0].Fields[$"ordars.{TOTAL_COLUMN}"]);
    }
    
    [TestMethod]
    public void Test_GetUsers_FollowOrdersLeft_Works()
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
        
        // Insert test users
        var johnResult = _connection.Execute($"upsert {USERS_TABLE} {{ id: 1, {NAME_COLUMN}: '{JOHN_NAME}' }}");
        Assert.IsTrue(johnResult.Success);
        var janeResult = _connection.Execute($"upsert {USERS_TABLE} {{ id: 2, {NAME_COLUMN}: '{JANE_NAME}' }}");
        Assert.IsTrue(janeResult.Success);
        var aliceResult = _connection.Execute($"upsert {USERS_TABLE} {{ id: 3, {NAME_COLUMN}: '{ALICE_NAME}' }}");
        Assert.IsTrue(aliceResult.Success);
        
        // Insert test orders - Alice has no orders
        var order1Result = _connection.Execute($"upsert {ORDERS_TABLE} {{ id: 101, user_id: 1, {TOTAL_COLUMN}: 150.50 }}");
        Assert.IsTrue(order1Result.Success);
        var order2Result = _connection.Execute($"upsert {ORDERS_TABLE} {{ id: 102, user_id: 1, {TOTAL_COLUMN}: 200.75 }}");
        Assert.IsTrue(order2Result.Success);
        var order3Result = _connection.Execute($"upsert {ORDERS_TABLE} {{ id: 103, user_id: 2, {TOTAL_COLUMN}: 75.25 }}");
        Assert.IsTrue(order3Result.Success);
        
        // Act
        var result = _connection.Execute($"get {USERS_TABLE} follow {USERS_TABLE}.id -> {ORDERS_TABLE}.user_id as ordars (left)");
        
        // Assert
        Assert.IsTrue(result.Success, $"Failed to left join users with orders: {result.Error}");
        Assert.IsNull(result.Error);
        
        // Verify the returned data
        Assert.IsNotNull(result.Data);
        var resultRows = result.Data as IList<Row>;
        Assert.IsNotNull(resultRows);
        
        // Should return one row for each joined pair plus users with no orders
        Assert.AreEqual(4, resultRows!.Count); // 2 orders for John, 1 for Jane, and 1 for Alice with null order values
        
        // Verify all users are present including those with no orders
        var userIds = resultRows.Select(r => (int)r.Fields["users.id"]).Distinct().ToList();
        Assert.AreEqual(3, userIds.Count);
        Assert.IsTrue(userIds.Contains(1)); // John
        Assert.IsTrue(userIds.Contains(2)); // Jane
        Assert.IsTrue(userIds.Contains(3)); // Alice
        
        // Verify Alice has null order values
        var aliceRow = resultRows.FirstOrDefault(r => (int)r.Fields["users.id"] == 3);
        Assert.IsNotNull(aliceRow);
        Assert.IsNull(aliceRow!.Fields["ordars.id"]);
        Assert.IsNull(aliceRow.Fields["ordars.user_id"]);
        Assert.IsNull(aliceRow.Fields[$"ordars.{TOTAL_COLUMN}"]);
    }
    
    [TestMethod]
    public void Test_GetUsers_FollowOrdersInner_Works()
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
        var addStatusColumnResult = _connection.Execute($"add column {ORDERS_TABLE}.status {STRING_TYPE}");
        Assert.IsTrue(addStatusColumnResult.Success);
        
        // Insert test users
        var johnResult = _connection.Execute($"upsert {USERS_TABLE} {{ id: 1, {NAME_COLUMN}: '{JOHN_NAME}' }}");
        Assert.IsTrue(johnResult.Success);
        var janeResult = _connection.Execute($"upsert {USERS_TABLE} {{ id: 2, {NAME_COLUMN}: '{JANE_NAME}' }}");
        Assert.IsTrue(janeResult.Success);
        var aliceResult = _connection.Execute($"upsert {USERS_TABLE} {{ id: 3, {NAME_COLUMN}: '{ALICE_NAME}' }}");
        Assert.IsTrue(aliceResult.Success);
        
        // Insert test orders - Alice has no orders
        var order1Result = _connection.Execute($"upsert {ORDERS_TABLE} {{ id: 101, user_id: 1, {TOTAL_COLUMN}: 150.50, status: 'completed' }}");
        Assert.IsTrue(order1Result.Success);
        var order2Result = _connection.Execute($"upsert {ORDERS_TABLE} {{ id: 102, user_id: 1, {TOTAL_COLUMN}: 200.75, status: 'processing' }}");
        Assert.IsTrue(order2Result.Success);
        var order3Result = _connection.Execute($"upsert {ORDERS_TABLE} {{ id: 103, user_id: 2, {TOTAL_COLUMN}: 75.25, status: 'completed' }}");
        Assert.IsTrue(order3Result.Success);
        
        // Act
        var result = _connection.Execute($"get {USERS_TABLE} follow {USERS_TABLE}.id -> {ORDERS_TABLE}.user_id as ordars (inner)");
        
        // Assert
        Assert.IsTrue(result.Success, $"Failed to inner join users with orders: {result.Error}");
        Assert.IsNull(result.Error);
        
        // Verify the returned data
        Assert.IsNotNull(result.Data);
        var resultRows = result.Data as IList<Row>;
        Assert.IsNotNull(resultRows);
        
        // Should return one row for each joined pair (only matched records)
        Assert.AreEqual(3, resultRows!.Count); // 2 orders for John, 1 for Jane, none for Alice
        
        // Verify only users with orders are present
        var userIds = resultRows.Select(r => (int)r.Fields["users.id"]).Distinct().ToList();
        Assert.AreEqual(2, userIds.Count);
        Assert.IsTrue(userIds.Contains(1)); // John
        Assert.IsTrue(userIds.Contains(2)); // Jane
        Assert.IsFalse(userIds.Contains(3)); // Alice should not be included (no orders)
    }
    
    [TestMethod]
    public void Test_GetUsers_FollowOrdersOnStatus_Works()
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
        var addStatusColumnResult = _connection.Execute($"add column {ORDERS_TABLE}.status {STRING_TYPE}");
        Assert.IsTrue(addStatusColumnResult.Success);
        
        // Insert test users
        var johnResult = _connection.Execute($"upsert {USERS_TABLE} {{ id: 1, {NAME_COLUMN}: '{JOHN_NAME}' }}");
        Assert.IsTrue(johnResult.Success);
        var janeResult = _connection.Execute($"upsert {USERS_TABLE} {{ id: 2, {NAME_COLUMN}: '{JANE_NAME}' }}");
        Assert.IsTrue(janeResult.Success);
        var aliceResult = _connection.Execute($"upsert {USERS_TABLE} {{ id: 3, {NAME_COLUMN}: '{ALICE_NAME}' }}");
        Assert.IsTrue(aliceResult.Success);
        
        // Insert test orders with different statuses
        var order1Result = _connection.Execute($"upsert {ORDERS_TABLE} {{ id: 101, user_id: 1, {TOTAL_COLUMN}: 150.50, status: 'completed' }}");
        Assert.IsTrue(order1Result.Success);
        var order2Result = _connection.Execute($"upsert {ORDERS_TABLE} {{ id: 102, user_id: 1, {TOTAL_COLUMN}: 200.75, status: 'processing' }}");
        Assert.IsTrue(order2Result.Success);
        var order3Result = _connection.Execute($"upsert {ORDERS_TABLE} {{ id: 103, user_id: 2, {TOTAL_COLUMN}: 75.25, status: 'completed' }}");
        Assert.IsTrue(order3Result.Success);
        
        // Act
        var result = _connection.Execute($"get {USERS_TABLE} follow {USERS_TABLE}.id -> {ORDERS_TABLE}.user_id as ordars on ordars.status = 'completed'");
        
        // Assert
        Assert.IsTrue(result.Success, $"Failed to join users with orders on status: {result.Error}");
        Assert.IsNull(result.Error);
        
        // Verify the returned data
        Assert.IsNotNull(result.Data);
        var resultRows = result.Data as IList<Row>;
        Assert.IsNotNull(resultRows);
        
        // Should return one row for each joined pair with status = 'completed'
        Assert.AreEqual(2, resultRows!.Count); // 1 completed order for John, 1 for Jane
        
        // Verify only completed orders are included
        Assert.IsTrue(resultRows.All(r => r.Fields["ordars.status"]?.ToString() == "completed"));
        
        // Verify the order IDs are correct
        var orderIds = resultRows.Select(r => (int)r.Fields["ordars.id"]).ToList();
        Assert.IsTrue(orderIds.Contains(101)); // John's completed order
        Assert.IsTrue(orderIds.Contains(103)); // Jane's completed order
        Assert.IsFalse(orderIds.Contains(102)); // John's processing order should not be included
    }
    
    [TestMethod]
    public void Test_GetUsers_FollowOrdersFollowItems_Works()
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
        
        // Create items table
        var createItemsTableResult = _connection.Execute($"create table items");
        Assert.IsTrue(createItemsTableResult.Success);
        var addItemIdColumnResult = _connection.Execute($"add column items.id {NUMBER_TYPE}");
        Assert.IsTrue(addItemIdColumnResult.Success);
        var addOrderIdFkColumnResult = _connection.Execute($"add column items.order_id {NUMBER_TYPE}");
        Assert.IsTrue(addOrderIdFkColumnResult.Success);
        var addNameItemColumnResult = _connection.Execute($"add column items.name {STRING_TYPE}");
        Assert.IsTrue(addNameItemColumnResult.Success);
        var addPriceColumnResult = _connection.Execute($"add column items.price {NUMBER_TYPE}");
        Assert.IsTrue(addPriceColumnResult.Success);
        
        // Insert test users
        var johnResult = _connection.Execute($"upsert {USERS_TABLE} {{ id: 1, {NAME_COLUMN}: '{JOHN_NAME}' }}");
        Assert.IsTrue(johnResult.Success);
        var janeResult = _connection.Execute($"upsert {USERS_TABLE} {{ id: 2, {NAME_COLUMN}: '{JANE_NAME}' }}");
        Assert.IsTrue(janeResult.Success);
        
        // Insert test orders
        var order1Result = _connection.Execute($"upsert {ORDERS_TABLE} {{ id: 101, user_id: 1, {TOTAL_COLUMN}: 150.50 }}");
        Assert.IsTrue(order1Result.Success);
        var order2Result = _connection.Execute($"upsert {ORDERS_TABLE} {{ id: 102, user_id: 2, {TOTAL_COLUMN}: 75.25 }}");
        Assert.IsTrue(order2Result.Success);
        
        // Insert test items
        var item1Result = _connection.Execute($"upsert items {{ id: 1001, order_id: 101, name: 'Laptop', price: 100.00 }}");
        Assert.IsTrue(item1Result.Success);
        var item2Result = _connection.Execute($"upsert items {{ id: 1002, order_id: 101, name: 'Mouse', price: 50.50 }}");
        Assert.IsTrue(item2Result.Success);
        var item3Result = _connection.Execute($"upsert items {{ id: 1003, order_id: 102, name: 'Keyboard', price: 75.25 }}");
        Assert.IsTrue(item3Result.Success);
        
        // Act
        var result = _connection.Execute($"get {USERS_TABLE} follow {USERS_TABLE}.id -> {ORDERS_TABLE}.user_id as ordars follow ordars.id -> items.order_id as itms");
        
        // Assert
        Assert.IsTrue(result.Success, $"Failed to join users with orders and items: {result.Error}");
        Assert.IsNull(result.Error);
        
        // Verify the returned data
        Assert.IsNotNull(result.Data);
        var resultRows = result.Data as IList<Row>;
        Assert.IsNotNull(resultRows);
        
        // Should return one row for each complete join path
        Assert.AreEqual(3, resultRows!.Count); // 2 items for John's order + 1 item for Jane's order
        
        // Verify that all fields from all tables are present
        Assert.IsTrue(resultRows.All(r => r.Fields.ContainsKey("users.id")));
        Assert.IsTrue(resultRows.All(r => r.Fields.ContainsKey($"users.{NAME_COLUMN}")));
        Assert.IsTrue(resultRows.All(r => r.Fields.ContainsKey("ordars.id")));
        Assert.IsTrue(resultRows.All(r => r.Fields.ContainsKey("ordars.user_id")));
        Assert.IsTrue(resultRows.All(r => r.Fields.ContainsKey($"ordars.{TOTAL_COLUMN}")));
        Assert.IsTrue(resultRows.All(r => r.Fields.ContainsKey("itms.id")));
        Assert.IsTrue(resultRows.All(r => r.Fields.ContainsKey("itms.order_id")));
        Assert.IsTrue(resultRows.All(r => r.Fields.ContainsKey("itms.name")));
        Assert.IsTrue(resultRows.All(r => r.Fields.ContainsKey("itms.price")));
        
        // Verify the correct relationships in the joined data
        var johnRows = resultRows.Where(r => (int)r.Fields["users.id"] == 1).ToList();
        var janeRows = resultRows.Where(r => (int)r.Fields["users.id"] == 2).ToList();
        
        Assert.AreEqual(2, johnRows.Count);
        Assert.AreEqual(1, janeRows.Count);
        
        // Verify John's items
        var johnItems = johnRows.Select(r => r.Fields["itms.name"]?.ToString()).ToList();
        Assert.IsTrue(johnItems.Contains("Laptop"));
        Assert.IsTrue(johnItems.Contains("Mouse"));
        
        // Verify Jane's item
        Assert.AreEqual("Keyboard", janeRows[0].Fields["itms.name"]);
    }
    
    [TestMethod]
    public void Test_GetUsers_FollowOrders_WhereOrderTotalGreaterThan100_Works()
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
        
        // Insert test users
        var johnResult = _connection.Execute($"upsert {USERS_TABLE} {{ id: 1, {NAME_COLUMN}: '{JOHN_NAME}' }}");
        Assert.IsTrue(johnResult.Success);
        var janeResult = _connection.Execute($"upsert {USERS_TABLE} {{ id: 2, {NAME_COLUMN}: '{JANE_NAME}' }}");
        Assert.IsTrue(janeResult.Success);
        
        // Insert test orders with different totals
        var order1Result = _connection.Execute($"upsert {ORDERS_TABLE} {{ id: 101, user_id: 1, {TOTAL_COLUMN}: 150.50 }}"); // > 100
        Assert.IsTrue(order1Result.Success);
        var order2Result = _connection.Execute($"upsert {ORDERS_TABLE} {{ id: 102, user_id: 1, {TOTAL_COLUMN}: 75.25 }}"); // < 100
        Assert.IsTrue(order2Result.Success);
        var order3Result = _connection.Execute($"upsert {ORDERS_TABLE} {{ id: 103, user_id: 2, {TOTAL_COLUMN}: 120.00 }}"); // > 100
        Assert.IsTrue(order3Result.Success);
        
        // Act
        var result = _connection.Execute($"get {USERS_TABLE} follow {USERS_TABLE}.id -> {ORDERS_TABLE}.user_id as orders where orders.{TOTAL_COLUMN} > 100");
        
        // Assert
        Assert.IsTrue(result.Success, $"Failed to join users with orders where total > 100: {result.Error}");
        Assert.IsNull(result.Error);
        
        // Verify the returned data
        Assert.IsNotNull(result.Data);
        var resultRows = result.Data as IList<Row>;
        Assert.IsNotNull(resultRows);
        
        // Should return only orders with total > 100
        Assert.AreEqual(2, resultRows!.Count); // 1 order for John, 1 for Jane
        
        // Verify that all returned orders have total > 100
        Assert.IsTrue(resultRows.All(r => (double)r.Fields[$"orders.{TOTAL_COLUMN}"] > 100));
        
        // Verify the correct order IDs are returned
        var orderIds = resultRows.Select(r => (int)r.Fields["orders.id"]).ToList();
        Assert.IsTrue(orderIds.Contains(101)); // John's order > 100
        Assert.IsTrue(orderIds.Contains(103)); // Jane's order > 100
        Assert.IsFalse(orderIds.Contains(102)); // John's order < 100 should not be included
    }
    
    [TestMethod]
    public void Test_GetUsers_FollowOrders_GroupByUserName_Works()
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
        
        // Insert test users
        var johnResult = _connection.Execute($"upsert {USERS_TABLE} {{ id: 1, {NAME_COLUMN}: '{JOHN_NAME}' }}");
        Assert.IsTrue(johnResult.Success);
        var janeResult = _connection.Execute($"upsert {USERS_TABLE} {{ id: 2, {NAME_COLUMN}: '{JANE_NAME}' }}");
        Assert.IsTrue(janeResult.Success);
        var aliceResult = _connection.Execute($"upsert {USERS_TABLE} {{ id: 3, {NAME_COLUMN}: '{ALICE_NAME}' }}");
        Assert.IsTrue(aliceResult.Success);
        
        // Insert test orders
        var order1Result = _connection.Execute($"upsert {ORDERS_TABLE} {{ id: 101, user_id: 1, {TOTAL_COLUMN}: 150.50 }}");
        Assert.IsTrue(order1Result.Success);
        var order2Result = _connection.Execute($"upsert {ORDERS_TABLE} {{ id: 102, user_id: 1, {TOTAL_COLUMN}: 200.75 }}");
        Assert.IsTrue(order2Result.Success);
        var order3Result = _connection.Execute($"upsert {ORDERS_TABLE} {{ id: 103, user_id: 1, {TOTAL_COLUMN}: 75.25 }}");
        Assert.IsTrue(order3Result.Success);
        var order4Result = _connection.Execute($"upsert {ORDERS_TABLE} {{ id: 104, user_id: 2, {TOTAL_COLUMN}: 120.00 }}");
        Assert.IsTrue(order4Result.Success);
        // Alice has no orders
        
        // Act
        var result = _connection.Execute($"get {USERS_TABLE} follow {USERS_TABLE}.id -> {ORDERS_TABLE}.user_id as orders group by {USERS_TABLE}.{NAME_COLUMN}");
        
        // Assert
        Assert.IsTrue(result.Success, $"Failed to join users with orders and group by name: {result.Error}");
        Assert.IsNull(result.Error);
        
        // Verify the returned data
        Assert.IsNotNull(result.Data);
        var resultRows = result.Data as IList<Row>;
        Assert.IsNotNull(resultRows);
        
        // Should return 2 groups (John and Jane) - Alice has no orders so is excluded in inner join
        Assert.AreEqual(2, resultRows!.Count);
        
        // Verify all expected users are present
        var userNames = resultRows.Select(r => r.Fields[NAME_COLUMN]?.ToString()).ToList();
        Assert.IsTrue(userNames.Contains(JOHN_NAME));
        Assert.IsTrue(userNames.Contains(JANE_NAME));
        Assert.IsFalse(userNames.Contains(ALICE_NAME)); // Alice has no orders (inner join)
    }
    
    [TestMethod]
    public void Test_GetUsers_FollowOrders_GroupByUserName_HavingCountOrdersGreaterThan2_Works()
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
        
        // Insert test users
        var johnResult = _connection.Execute($"upsert {USERS_TABLE} {{ id: 1, {NAME_COLUMN}: '{JOHN_NAME}' }}");
        Assert.IsTrue(johnResult.Success);
        var janeResult = _connection.Execute($"upsert {USERS_TABLE} {{ id: 2, {NAME_COLUMN}: '{JANE_NAME}' }}");
        Assert.IsTrue(janeResult.Success);
        
        // Insert test orders - John has 3 orders, Jane has 1
        var order1Result = _connection.Execute($"upsert {ORDERS_TABLE} {{ id: 101, user_id: 1, {TOTAL_COLUMN}: 150.50 }}");
        Assert.IsTrue(order1Result.Success);
        var order2Result = _connection.Execute($"upsert {ORDERS_TABLE} {{ id: 102, user_id: 1, {TOTAL_COLUMN}: 200.75 }}");
        Assert.IsTrue(order2Result.Success);
        var order3Result = _connection.Execute($"upsert {ORDERS_TABLE} {{ id: 103, user_id: 1, {TOTAL_COLUMN}: 75.25 }}");
        Assert.IsTrue(order3Result.Success);
        var order4Result = _connection.Execute($"upsert {ORDERS_TABLE} {{ id: 104, user_id: 2, {TOTAL_COLUMN}: 120.00 }}");
        Assert.IsTrue(order4Result.Success);
        
        // Act
        var result = _connection.Execute($"get {USERS_TABLE} follow {USERS_TABLE}.id -> {ORDERS_TABLE}.user_id as orders group by {USERS_TABLE}.{NAME_COLUMN} having count(orders) > 2");
        
        // Assert
        Assert.IsTrue(result.Success, $"Failed to join and group with having clause: {result.Error}");
        Assert.IsNull(result.Error);
        
        // Verify the returned data
        Assert.IsNotNull(result.Data);
        var resultRows = result.Data as IList<Row>;
        Assert.IsNotNull(resultRows);
        
        // Should return only John's group (with > 2 orders)
        Assert.AreEqual(1, resultRows!.Count);
        Assert.AreEqual(JOHN_NAME, resultRows[0].Fields[NAME_COLUMN]);
    }
    
    [TestMethod]
    public void Test_GetUsers_FollowOrders_ComplexQuery_Works()
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
        
        // Insert test users
        var johnResult = _connection.Execute($"upsert {USERS_TABLE} {{ id: 1, {NAME_COLUMN}: '{JOHN_NAME}' }}");
        Assert.IsTrue(johnResult.Success);
        var janeResult = _connection.Execute($"upsert {USERS_TABLE} {{ id: 2, {NAME_COLUMN}: '{JANE_NAME}' }}");
        Assert.IsTrue(janeResult.Success);
        var aliceResult = _connection.Execute($"upsert {USERS_TABLE} {{ id: 3, {NAME_COLUMN}: '{ALICE_NAME}' }}");
        Assert.IsTrue(aliceResult.Success);
        
        // Insert test orders
        // John has 3 orders > 100
        var order1Result = _connection.Execute($"upsert {ORDERS_TABLE} {{ id: 101, user_id: 1, {TOTAL_COLUMN}: 150.50 }}");
        Assert.IsTrue(order1Result.Success);
        var order2Result = _connection.Execute($"upsert {ORDERS_TABLE} {{ id: 102, user_id: 1, {TOTAL_COLUMN}: 200.75 }}");
        Assert.IsTrue(order2Result.Success);
        var order3Result = _connection.Execute($"upsert {ORDERS_TABLE} {{ id: 103, user_id: 1, {TOTAL_COLUMN}: 175.25 }}");
        Assert.IsTrue(order3Result.Success);
        
        // Jane has 2 orders > 100
        var order4Result = _connection.Execute($"upsert {ORDERS_TABLE} {{ id: 104, user_id: 2, {TOTAL_COLUMN}: 120.00 }}");
        Assert.IsTrue(order4Result.Success);
        var order5Result = _connection.Execute($"upsert {ORDERS_TABLE} {{ id: 105, user_id: 2, {TOTAL_COLUMN}: 130.00 }}");
        Assert.IsTrue(order5Result.Success);
        
        // Alice has 1 order > 100
        var order6Result = _connection.Execute($"upsert {ORDERS_TABLE} {{ id: 106, user_id: 3, {TOTAL_COLUMN}: 110.00 }}");
        Assert.IsTrue(order6Result.Success);
        
        // Act - complex query with join, where, group by, having, order by, select with aggregates
        var query = $@"get {USERS_TABLE} 
                      follow {USERS_TABLE}.id -> {ORDERS_TABLE}.user_id as orders 
                      where orders.{TOTAL_COLUMN} > 100 
                      group by {USERS_TABLE}.{NAME_COLUMN} 
                      having count(orders) > 2 
                      order by count(orders) desc 
                      select {USERS_TABLE}.{NAME_COLUMN}, count(orders) as order_count, sum(orders.{TOTAL_COLUMN}) as total_spent";
        
        var result = _connection.Execute(query);
        
        // Assert
        Assert.IsTrue(result.Success, $"Failed to execute complex join query: {result.Error}");
        Assert.IsNull(result.Error);
        
        // Verify the returned data
        Assert.IsNotNull(result.Data);
        var resultRows = result.Data as IList<Row>;
        Assert.IsNotNull(resultRows);
        
        // Should return only John's data (only user with > 2 orders over 100)
        Assert.AreEqual(1, resultRows!.Count);
        
        // Verify the returned columns match the select clause
        var row = resultRows[0];
        Assert.AreEqual(3, row.Fields.Count);
        Assert.IsTrue(row.Fields.ContainsKey(NAME_COLUMN));
        Assert.IsTrue(row.Fields.ContainsKey("order_count"));
        Assert.IsTrue(row.Fields.ContainsKey("total_spent"));
        
        // Verify the values are correct
        Assert.AreEqual(JOHN_NAME, row.Fields[NAME_COLUMN]);
        Assert.AreEqual(3, row.Fields["order_count"]);
        Assert.AreEqual(150.50 + 200.75 + 175.25, (double)row.Fields["total_spent"], 0.001);
    }
}
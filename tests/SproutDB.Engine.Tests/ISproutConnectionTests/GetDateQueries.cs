using SproutDB.Engine.Core;

namespace SproutDB.Engine.Tests.ISproutConnectionTests;

[TestClass]
public class GetDateQueries : ISproutConnectionTestsSetup
{
    [TestMethod]
    public void Test_GetOrders_WhereDateLastSevenDays_Works()
    {
        // Arrange
        var createDbResult = _connection.Execute($"create database {TEST_DATABASE}");
        Assert.IsTrue(createDbResult.Success);
        var createTableResult = _connection.Execute($"create table {ORDERS_TABLE}");
        Assert.IsTrue(createTableResult.Success);
        var addDateColumnResult = _connection.Execute($"add column {ORDERS_TABLE}.{DATE_COLUMN} {STRING_TYPE}");
        Assert.IsTrue(addDateColumnResult.Success);
        var addTotalColumnResult = _connection.Execute($"add column {ORDERS_TABLE}.{TOTAL_COLUMN} {NUMBER_TYPE}");
        Assert.IsTrue(addTotalColumnResult.Success);

        // Insert test orders with different dates
        var today = DateTime.Now.ToString("yyyy-MM-dd");
        var sevenDaysAgo = DateTime.Now.AddDays(-7).ToString("yyyy-MM-dd");
        var tenDaysAgo = DateTime.Now.AddDays(-10).ToString("yyyy-MM-dd");

        var order1Result = _connection.Execute($"upsert {ORDERS_TABLE} {{ {DATE_COLUMN}: '{today}', {TOTAL_COLUMN}: 100 }}");
        Assert.IsTrue(order1Result.Success);
        var order2Result = _connection.Execute($"upsert {ORDERS_TABLE} {{ {DATE_COLUMN}: '{sevenDaysAgo}', {TOTAL_COLUMN}: 200 }}");
        Assert.IsTrue(order2Result.Success);
        var order3Result = _connection.Execute($"upsert {ORDERS_TABLE} {{ {DATE_COLUMN}: '{tenDaysAgo}', {TOTAL_COLUMN}: 300 }}");
        Assert.IsTrue(order3Result.Success);

        // Act
        var result = _connection.Execute($"get {ORDERS_TABLE} where {DATE_COLUMN} last 7 days");

        // Assert
        Assert.IsTrue(result.Success, $"Failed to get orders from last 7 days: {result.Error}");
        Assert.IsNull(result.Error);

        // Verify the returned data
        Assert.IsNotNull(result.Data);
        var resultRows = result.Data as IList<Row>;
        Assert.IsNotNull(resultRows);

        // Should return only orders from today and seven days ago, not 10 days ago
        Assert.AreEqual(2, resultRows!.Count);

        // Check that we got the right orders
        var dates = resultRows.Select(r => r.Fields[DATE_COLUMN]?.ToString()).ToList();
        Assert.IsTrue(dates.Contains(today));
        Assert.IsTrue(dates.Contains(sevenDaysAgo));
        Assert.IsFalse(dates.Contains(tenDaysAgo));
    }

    [TestMethod]
    public void Test_GetOrders_WhereDateThisMonth_Works()
    {
        // Arrange
        var createDbResult = _connection.Execute($"create database {TEST_DATABASE}");
        Assert.IsTrue(createDbResult.Success);
        var createTableResult = _connection.Execute($"create table {ORDERS_TABLE}");
        Assert.IsTrue(createTableResult.Success);
        var addDateColumnResult = _connection.Execute($"add column {ORDERS_TABLE}.{DATE_COLUMN} {STRING_TYPE}");
        Assert.IsTrue(addDateColumnResult.Success);
        var addTotalColumnResult = _connection.Execute($"add column {ORDERS_TABLE}.{TOTAL_COLUMN} {NUMBER_TYPE}");
        Assert.IsTrue(addTotalColumnResult.Success);

        // Insert test orders with different dates
        var thisMonth = DateTime.Now.ToString("yyyy-MM-dd");
        var lastMonth = DateTime.Now.AddMonths(-1).ToString("yyyy-MM-dd");

        var order1Result = _connection.Execute($"upsert {ORDERS_TABLE} {{ {DATE_COLUMN}: '{thisMonth}', {TOTAL_COLUMN}: 100 }}");
        Assert.IsTrue(order1Result.Success);
        var order2Result = _connection.Execute($"upsert {ORDERS_TABLE} {{ {DATE_COLUMN}: '{lastMonth}', {TOTAL_COLUMN}: 200 }}");
        Assert.IsTrue(order2Result.Success);

        // Act
        var result = _connection.Execute($"get {ORDERS_TABLE} where {DATE_COLUMN} this month");

        // Assert
        Assert.IsTrue(result.Success, $"Failed to get orders from this month: {result.Error}");
        Assert.IsNull(result.Error);

        // Verify the returned data
        Assert.IsNotNull(result.Data);
        var resultRows = result.Data as IList<Row>;
        Assert.IsNotNull(resultRows);

        // Should return only orders from this month
        Assert.AreEqual(1, resultRows!.Count);

        // Check that we got the right order
        var dates = resultRows.Select(r => r.Fields[DATE_COLUMN]?.ToString()).ToList();
        Assert.IsTrue(dates.Contains(thisMonth));
        Assert.IsFalse(dates.Contains(lastMonth));
    }

    [TestMethod]
    public void Test_GetOrders_WhereDateBefore_Works()
    {
        // Arrange
        var createDbResult = _connection.Execute($"create database {TEST_DATABASE}");
        Assert.IsTrue(createDbResult.Success);
        var createTableResult = _connection.Execute($"create table {ORDERS_TABLE}");
        Assert.IsTrue(createTableResult.Success);
        var addDateColumnResult = _connection.Execute($"add column {ORDERS_TABLE}.{DATE_COLUMN} {STRING_TYPE}");
        Assert.IsTrue(addDateColumnResult.Success);
        var addTotalColumnResult = _connection.Execute($"add column {ORDERS_TABLE}.{TOTAL_COLUMN} {NUMBER_TYPE}");
        Assert.IsTrue(addTotalColumnResult.Success);

        // Insert test orders with different dates
        var beforeDate = "2024-07-15";
        var afterDate = "2024-08-15";
        var cutoffDate = "2024-08-01";

        var order1Result = _connection.Execute($"upsert {ORDERS_TABLE} {{ {DATE_COLUMN}: '{beforeDate}', {TOTAL_COLUMN}: 100 }}");
        Assert.IsTrue(order1Result.Success);
        var order2Result = _connection.Execute($"upsert {ORDERS_TABLE} {{ {DATE_COLUMN}: '{afterDate}', {TOTAL_COLUMN}: 200 }}");
        Assert.IsTrue(order2Result.Success);

        // Act
        var result = _connection.Execute($"get {ORDERS_TABLE} where {DATE_COLUMN} before '{cutoffDate}'");

        // Assert
        Assert.IsTrue(result.Success, $"Failed to get orders before date: {result.Error}");
        Assert.IsNull(result.Error);

        // Verify the returned data
        Assert.IsNotNull(result.Data);
        var resultRows = result.Data as IList<Row>;
        Assert.IsNotNull(resultRows);

        // Should return only orders before the cutoff date
        Assert.AreEqual(1, resultRows!.Count);

        // Check that we got the right order
        Assert.AreEqual(beforeDate, resultRows[0].Fields[DATE_COLUMN]);
        Assert.AreEqual(100, resultRows[0].Fields[TOTAL_COLUMN]);
    }

    [TestMethod]
    public void Test_GetOrders_WhereDateAfter_Works()
    {
        // Arrange
        var createDbResult = _connection.Execute($"create database {TEST_DATABASE}");
        Assert.IsTrue(createDbResult.Success);
        var createTableResult = _connection.Execute($"create table {ORDERS_TABLE}");
        Assert.IsTrue(createTableResult.Success);
        var addDateColumnResult = _connection.Execute($"add column {ORDERS_TABLE}.{DATE_COLUMN} {STRING_TYPE}");
        Assert.IsTrue(addDateColumnResult.Success);
        var addTotalColumnResult = _connection.Execute($"add column {ORDERS_TABLE}.{TOTAL_COLUMN} {NUMBER_TYPE}");
        Assert.IsTrue(addTotalColumnResult.Success);

        // Insert test orders with different dates
        var beforeDate = "2024-07-10";
        var afterDate = "2024-07-20";
        var cutoffDate = "2024-07-15";

        var order1Result = _connection.Execute($"upsert {ORDERS_TABLE} {{ {DATE_COLUMN}: '{beforeDate}', {TOTAL_COLUMN}: 100 }}");
        Assert.IsTrue(order1Result.Success);
        var order2Result = _connection.Execute($"upsert {ORDERS_TABLE} {{ {DATE_COLUMN}: '{afterDate}', {TOTAL_COLUMN}: 200 }}");
        Assert.IsTrue(order2Result.Success);

        // Act
        var result = _connection.Execute($"get {ORDERS_TABLE} where {DATE_COLUMN} after '{cutoffDate}'");

        // Assert
        Assert.IsTrue(result.Success, $"Failed to get orders after date: {result.Error}");
        Assert.IsNull(result.Error);

        // Verify the returned data
        Assert.IsNotNull(result.Data);
        var resultRows = result.Data as IList<Row>;
        Assert.IsNotNull(resultRows);

        // Should return only orders after the cutoff date
        Assert.AreEqual(1, resultRows!.Count);

        // Check that we got the right order
        Assert.AreEqual(afterDate, resultRows[0].Fields[DATE_COLUMN]);
        Assert.AreEqual(200, resultRows[0].Fields[TOTAL_COLUMN]);
    }

    [TestMethod]
    public void Test_GetOrders_WhereDateBetween_Works()
    {
        // Arrange
        var createDbResult = _connection.Execute($"create database {TEST_DATABASE}");
        Assert.IsTrue(createDbResult.Success);
        var createTableResult = _connection.Execute($"create table {ORDERS_TABLE}");
        Assert.IsTrue(createTableResult.Success);
        var addDateColumnResult = _connection.Execute($"add column {ORDERS_TABLE}.{DATE_COLUMN} {STRING_TYPE}");
        Assert.IsTrue(addDateColumnResult.Success);
        var addTotalColumnResult = _connection.Execute($"add column {ORDERS_TABLE}.{TOTAL_COLUMN} {NUMBER_TYPE}");
        Assert.IsTrue(addTotalColumnResult.Success);

        // Insert test orders with different dates
        var beforeRange = "2024-06-15";
        var inRange = "2024-07-15";
        var afterRange = "2024-08-15";
        var startDate = "2024-07-01";
        var endDate = "2024-08-01";

        var order1Result = _connection.Execute($"upsert {ORDERS_TABLE} {{ {DATE_COLUMN}: '{beforeRange}', {TOTAL_COLUMN}: 100 }}");
        Assert.IsTrue(order1Result.Success);
        var order2Result = _connection.Execute($"upsert {ORDERS_TABLE} {{ {DATE_COLUMN}: '{inRange}', {TOTAL_COLUMN}: 200 }}");
        Assert.IsTrue(order2Result.Success);
        var order3Result = _connection.Execute($"upsert {ORDERS_TABLE} {{ {DATE_COLUMN}: '{afterRange}', {TOTAL_COLUMN}: 300 }}");
        Assert.IsTrue(order3Result.Success);

        // Act
        var result = _connection.Execute($"get {ORDERS_TABLE} where {DATE_COLUMN} > '{startDate}' and {DATE_COLUMN} < '{endDate}'");

        // Assert
        Assert.IsTrue(result.Success, $"Failed to get orders between dates: {result.Error}");
        Assert.IsNull(result.Error);

        // Verify the returned data
        Assert.IsNotNull(result.Data);
        var resultRows = result.Data as IList<Row>;
        Assert.IsNotNull(resultRows);

        // Should return only orders within the date range
        Assert.AreEqual(1, resultRows!.Count);

        // Check that we got the right order
        Assert.AreEqual(inRange, resultRows[0].Fields[DATE_COLUMN]);
        Assert.AreEqual(200, resultRows[0].Fields[TOTAL_COLUMN]);
    }
}

/*
--create database testdb
--create table users
--add column users.name string
--add column users.age number
--add column users.active boolean
--drop table oldtable 
--get users
--get users select name, age
--get users as u select u.name, u.age
--get users where age > 25
--get users where age >= 30
--get users where age < 40
--get users where age <= 35
--get users where name = 'John Doe'
--get users where name != 'Jane Smith'
--get users where age > 25 and active = true
--get users where name = 'John' or name = 'Jane'
--get users where not active = false
--get users where name in ['John', 'Jane', 'Bob']
--get users where name contains 'oh'

count users
count users where age > 30
sum users.age
sum users.age where active = true
avg users.age
avg users.age where name contains 'J'

get users 
  follow users.id -> orders.user_id as orders 
  where orders.total > 100 
  group by users.name 
  having count(orders) > 2 
  order by count(orders) desc 
  select users.name, count(orders) as order_count, sum(orders.total) as total_spent
  page 1 size 10

--upsert users { name: 'Alice Cooper', age: 42, active: true }
--upsert users { name: 'Bob Johnson', age: 38, active: true }
--upsert users { name: 'Alice Cooper', age: 43, active: false } on name
--delete users where name = 'Bob Johnson'

get users follow users.id -> orders.user_id as orders
get users follow users.id -> orders.user_id as orders (left)
get users follow users.id -> orders.user_id as orders (inner)
get users follow users.id -> orders.user_id as orders on orders.status = 'completed'
get users follow users.id -> orders.user_id as orders follow orders.id -> items.order_id as items

get users group by active
get users group by active select active, count() as count
get users group by active having count() > 2
get orders group by user_id select user_id, sum(total) as total_spent

--get users order by age
--get users order by age desc
--get users order by name asc, age desc

get users page 1 size 10
get users order by age desc page 2 size 5

--get orders where date last 7 days
--get orders where date this month
--get orders where date before '2024-08-01'
--get orders where date after '2024-07-15'
--get orders where date > '2024-07-01' and date < '2024-08-01'

get users 
  follow users.id -> orders.user_id as orders 
  where orders.total > 100 
  group by users.name 
  having count(orders) > 2 
  order by count(orders) desc 
  select users.name, count(orders) as order_count, sum(orders.total) as total_spent
  page 1 size 10

--upsert users [
--  { name: 'John Doe', age: 30, active: true },
--  { name: 'Jane Smith', age: 25, active: true },
--  { name: 'Bob Johnson', age: 40, active: false }
--]
--upsert users { 
--  name: 'Alice Green', 
--  age: 28, 
--  active: true, 
--  profile: { 
--    settings: { theme: 'dark', notifications: true },
--    preferences: ['email', 'sms']
--  }
--}

get users where profile.settings.theme = 'dark'

*/
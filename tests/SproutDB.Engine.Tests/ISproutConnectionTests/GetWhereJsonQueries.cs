using SproutDB.Engine.Compilation;
using SproutDB.Engine.Core;
using static SproutDB.Engine.Compilation.Expression;

namespace SproutDB.Engine.Tests.ISproutConnectionTests;

[TestClass]
public class GetWhereJsonQueries : ISproutConnectionTestsSetup
{
    [TestMethod]
    public void Test_GetUsers_WhereProfileSettingsThemeIsDark_Works()
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
        var addProfileColumnResult = _connection.Execute($"add column {USERS_TABLE}.{PROFILE_COLUMN} {MIXED_TYPE}");
        Assert.IsTrue(addProfileColumnResult.Success);

        // Insert test users with different profile settings
        // User with dark theme
        var aliceGreenResult = _connection.Execute($@"upsert {USERS_TABLE} {{ 
            {NAME_COLUMN}: '{ALICE_GREEN_NAME}', 
            {AGE_COLUMN}: {ALICE_GREEN_AGE}, 
            {ACTIVE_COLUMN}: {ALICE_GREEN_ACTIVE.ToString().ToLower()},
            {PROFILE_COLUMN}: {{ 
                {SETTINGS_FIELD}: {{ 
                    {THEME_FIELD}: '{DARK_THEME}', 
                    {NOTIFICATIONS_FIELD}: {NOTIFICATIONS_ENABLED.ToString().ToLower()}
                }},
                {PREFERENCES_FIELD}: ['{PREFERENCE_EMAIL}', '{PREFERENCE_SMS}']
            }}
        }}");
        Assert.IsTrue(aliceGreenResult.Success);

        // User with light theme
        var johnResult = _connection.Execute($@"upsert {USERS_TABLE} {{ 
            {NAME_COLUMN}: '{JOHN_NAME}', 
            {AGE_COLUMN}: {JOHN_AGE}, 
            {ACTIVE_COLUMN}: {JOHN_ACTIVE.ToString().ToLower()},
            {PROFILE_COLUMN}: {{ 
                {SETTINGS_FIELD}: {{ 
                    {THEME_FIELD}: 'light', 
                    {NOTIFICATIONS_FIELD}: true
                }},
                {PREFERENCES_FIELD}: ['{PREFERENCE_EMAIL}']
            }}
        }}");
        Assert.IsTrue(johnResult.Success);

        // User with no theme specified
        var janeResult = _connection.Execute($@"upsert {USERS_TABLE} {{ 
            {NAME_COLUMN}: '{JANE_NAME}', 
            {AGE_COLUMN}: {JANE_AGE}, 
            {ACTIVE_COLUMN}: {JANE_ACTIVE.ToString().ToLower()},
            {PROFILE_COLUMN}: {{ 
                {PREFERENCES_FIELD}: ['{PREFERENCE_SMS}']
            }}
        }}");
        Assert.IsTrue(janeResult.Success);

        // Act
        var result = _connection.Execute($"get {USERS_TABLE} where {PROFILE_COLUMN}.{SETTINGS_FIELD}.{THEME_FIELD} = '{DARK_THEME}'");

        // Assert
        Assert.IsTrue(result.Success, $"Failed to get users with dark theme: {result.Error}");
        Assert.IsNull(result.Error);

        // Verify the returned data
        Assert.IsNotNull(result.Data);
        var resultRows = result.Data as IList<Row>;
        Assert.IsNotNull(resultRows);

        // Should return only the user with dark theme (Alice Green)
        Assert.AreEqual(1, resultRows!.Count);

        // Verify the correct user was returned
        var userRow = resultRows[0];
        Assert.AreEqual(ALICE_GREEN_NAME, userRow.Fields[NAME_COLUMN]);
        Assert.AreEqual(ALICE_GREEN_AGE, userRow.Fields[AGE_COLUMN]);
        Assert.AreEqual(ALICE_GREEN_ACTIVE, userRow.Fields[ACTIVE_COLUMN]);

        // Verify the profile data is present
        Assert.IsNotNull(userRow.Fields[PROFILE_COLUMN]);

        // Additional verification of the nested profile structure if needed
        var profile = userRow.Fields[PROFILE_COLUMN] as Dictionary<string, Expression>;
        Assert.IsNotNull(profile);

        var settings = profile![SETTINGS_FIELD].As<JsonData>().Value as Dictionary<string, Expression>;
        Assert.IsNotNull(settings);

        Assert.AreEqual(DARK_THEME, settings![THEME_FIELD].As<JsonData>().Value);
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
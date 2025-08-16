using SproutDB.Engine.Compilation;
using System.Linq;
using static SproutDB.Engine.Compilation.Expression;

namespace SproutDB.Engine.Tests.ISproutConnectionTests;

[TestClass]
public class UpsertQueries : ISproutConnectionTestsSetup
{
    [TestMethod]
    public void Test_Upsert_SingleRecord_Works()
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

        // Act
        var result = _connection.Execute($"upsert {USERS_TABLE} {{ {NAME_COLUMN}: '{ALICE_NAME}', {AGE_COLUMN}: {ALICE_AGE}, {ACTIVE_COLUMN}: {ALICE_ACTIVE.ToString().ToLower()} }}");

        // Assert
        Assert.IsTrue(result.Success, $"Failed to upsert record: {result.Error}");
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

    [TestMethod]
    public void Test_Upsert_MultipleRecords_Works()
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

        // Act
        var aliceResult = _connection.Execute($"upsert {USERS_TABLE} {{ {NAME_COLUMN}: '{ALICE_NAME}', {AGE_COLUMN}: {ALICE_AGE}, {ACTIVE_COLUMN}: {ALICE_ACTIVE.ToString().ToLower()} }}");
        var bobResult = _connection.Execute($"upsert {USERS_TABLE} {{ {NAME_COLUMN}: '{BOB_NAME}', {AGE_COLUMN}: {BOB_AGE}, {ACTIVE_COLUMN}: {BOB_ACTIVE.ToString().ToLower()} }}");

        // Assert
        Assert.IsTrue(aliceResult.Success, $"Failed to upsert Alice record: {aliceResult.Error}");
        Assert.AreEqual(1, aliceResult.RowsAffected);

        Assert.IsTrue(bobResult.Success, $"Failed to upsert Bob record: {bobResult.Error}");
        Assert.AreEqual(1, bobResult.RowsAffected);

        var database = _server.Databases[TEST_DATABASE];
        var table = database.Tables[USERS_TABLE];
        Assert.AreEqual(2, table.Rows.Count);

        var aliceRow = table.Rows.Values.FirstOrDefault(r => r.Fields[NAME_COLUMN]?.Equals(ALICE_NAME) is true);
        Assert.IsNotNull(aliceRow);
        Assert.AreEqual(ALICE_AGE, aliceRow.Fields[AGE_COLUMN]);
        Assert.AreEqual(ALICE_ACTIVE, aliceRow.Fields[ACTIVE_COLUMN]);

        var bobRow = table.Rows.Values.FirstOrDefault(r => r.Fields[NAME_COLUMN]?.Equals(BOB_NAME) is true);
        Assert.IsNotNull(bobRow);
        Assert.AreEqual(BOB_AGE, bobRow.Fields[AGE_COLUMN]);
        Assert.AreEqual(BOB_ACTIVE, bobRow.Fields[ACTIVE_COLUMN]);
    }

    [TestMethod]
    public void Test_Upsert_WithOnClause_Updates_ExistingRecord()
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
        var initialUpsertResult = _connection.Execute($"upsert {USERS_TABLE} {{ {NAME_COLUMN}: '{ALICE_NAME}', {AGE_COLUMN}: {ALICE_AGE}, {ACTIVE_COLUMN}: {ALICE_ACTIVE.ToString().ToLower()} }}");
        Assert.IsTrue(initialUpsertResult.Success);

        // Act
        var result = _connection.Execute($"upsert {USERS_TABLE} {{ {NAME_COLUMN}: '{ALICE_NAME}', {AGE_COLUMN}: {ALICE_UPDATED_AGE}, {ACTIVE_COLUMN}: {ALICE_UPDATED_ACTIVE.ToString().ToLower()} }} on {NAME_COLUMN}");

        // Assert
        Assert.IsTrue(result.Success, $"Failed to update existing record: {result.Error}");
        Assert.IsNull(result.Error);
        Assert.AreEqual(1, result.RowsAffected);
        var database = _server.Databases[TEST_DATABASE];
        var table = database.Tables[USERS_TABLE];
        Assert.AreEqual(1, table.Rows.Count);
        var row = table.Rows.First().Value;
        Assert.AreEqual(ALICE_NAME, row.Fields[NAME_COLUMN]);
        Assert.AreEqual(ALICE_UPDATED_AGE, row.Fields[AGE_COLUMN]);
        Assert.AreEqual(ALICE_UPDATED_ACTIVE, row.Fields[ACTIVE_COLUMN]);
    }

    [TestMethod]
    public void Test_Upsert_BatchRecords_Works()
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

        // Act
        var result = _connection.Execute($"upsert {USERS_TABLE} [\n" +
                            $"  {{ {NAME_COLUMN}: '{JOHN_NAME}', {AGE_COLUMN}: {JOHN_AGE}, {ACTIVE_COLUMN}: {JOHN_ACTIVE.ToString().ToLower()} }},\n" +
                            $"  {{ {NAME_COLUMN}: '{JANE_NAME}', {AGE_COLUMN}: {JANE_AGE}, {ACTIVE_COLUMN}: {JANE_ACTIVE.ToString().ToLower()} }},\n" +
                            $"  {{ {NAME_COLUMN}: '{BOB_NAME}', {AGE_COLUMN}: {BATCH_BOB_AGE}, {ACTIVE_COLUMN}: {BATCH_BOB_ACTIVE.ToString().ToLower()} }}\n" +
                            $"]");

        // Assert
        Assert.IsTrue(result.Success, $"Failed to batch upsert records: {result.Error}");
        Assert.IsNull(result.Error);
        Assert.AreEqual(3, result.RowsAffected);
        var database = _server.Databases[TEST_DATABASE];
        var table = database.Tables[USERS_TABLE];
        Assert.AreEqual(3, table.Rows.Count);

        // Check John's record
        var johnRow = table.Rows.Values.FirstOrDefault(r => r.Fields[NAME_COLUMN]?.Equals(JOHN_NAME) is true);
        Assert.IsNotNull(johnRow);
        Assert.AreEqual(JOHN_AGE, johnRow.Fields[AGE_COLUMN]);
        Assert.AreEqual(JOHN_ACTIVE, johnRow.Fields[ACTIVE_COLUMN]);

        // Check Jane's record
        var janeRow = table.Rows.Values.FirstOrDefault(r => r.Fields[NAME_COLUMN]?.Equals(JANE_NAME) is true);
        Assert.IsNotNull(janeRow);
        Assert.AreEqual(JANE_AGE, janeRow.Fields[AGE_COLUMN]);
        Assert.AreEqual(JANE_ACTIVE, janeRow.Fields[ACTIVE_COLUMN]);

        // Check Bob's record
        var bobRow = table.Rows.Values.FirstOrDefault(r => r.Fields[NAME_COLUMN]?.Equals(BOB_NAME) is true);
        Assert.IsNotNull(bobRow);
        Assert.AreEqual(BATCH_BOB_AGE, bobRow.Fields[AGE_COLUMN]);
        Assert.AreEqual(BATCH_BOB_ACTIVE, bobRow.Fields[ACTIVE_COLUMN]);
    }

    [TestMethod]
    public void Test_Upsert_NestedObject_Works()
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

        // Act
        var result = _connection.Execute($@"
            upsert {USERS_TABLE} {{
                {NAME_COLUMN}: '{ALICE_GREEN_NAME}', 
                {AGE_COLUMN}: {ALICE_GREEN_AGE}, 
                {ACTIVE_COLUMN}: {ALICE_GREEN_ACTIVE.ToString().ToLower()}, 
                {PROFILE_COLUMN}: {{
                    {SETTINGS_FIELD}: {{ {THEME_FIELD}: '{DARK_THEME}', {NOTIFICATIONS_FIELD}: {NOTIFICATIONS_ENABLED.ToString().ToLower()} }},
                    {PREFERENCES_FIELD}: ['{PREFERENCE_EMAIL}', '{PREFERENCE_SMS}']
                }}
            }}
        ");

        // Assert
        Assert.IsTrue(result.Success, $"Failed to upsert nested object: {result.Error}");
        Assert.IsNull(result.Error);
        Assert.AreEqual(1, result.RowsAffected);
        var database = _server.Databases[TEST_DATABASE];
        var table = database.Tables[USERS_TABLE];
        Assert.AreEqual(1, table.Rows.Count);
        var row = table.Rows.First().Value;
        Assert.AreEqual(ALICE_GREEN_NAME, row.Fields[NAME_COLUMN]);
        Assert.AreEqual(ALICE_GREEN_AGE, row.Fields[AGE_COLUMN]);
        Assert.AreEqual(ALICE_GREEN_ACTIVE, row.Fields[ACTIVE_COLUMN]);

        // Verify nested object fields
        var profile = row.Fields[PROFILE_COLUMN] as Dictionary<string, Expression>;
        Assert.IsNotNull(profile);
        Assert.IsTrue(profile!.ContainsKey(SETTINGS_FIELD));
        Assert.IsTrue(profile.ContainsKey(PREFERENCES_FIELD));

        // Verify settings fields
        var settings = profile[SETTINGS_FIELD].As<JsonData>().Value as Dictionary<string, Expression>;
        Assert.IsNotNull(settings);
        Assert.IsTrue(settings!.ContainsKey(THEME_FIELD));
        Assert.IsTrue(settings.ContainsKey(NOTIFICATIONS_FIELD));
        Assert.AreEqual(DARK_THEME, settings[THEME_FIELD].As<JsonData>().Value as string);
        Assert.AreEqual(NOTIFICATIONS_ENABLED.ToString().ToLower(), settings[NOTIFICATIONS_FIELD].As<JsonData>().Value as string);

        // Verify preferences fields
        var preferences = profile[PREFERENCES_FIELD].As<JsonData>().Value as Expression[];
        Assert.IsNotNull(preferences);
        Assert.AreEqual(2, preferences!.Count());
        Assert.IsTrue(preferences.Any(p => (p.As<JsonData>().Value as string) == PREFERENCE_EMAIL));
        Assert.IsTrue(preferences.Any(p => (p.As<JsonData>().Value as string) == PREFERENCE_SMS));
    }

}

using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using SproutDB.Engine.Compilation;
using SproutDB.Engine.Core;
using SproutDB.Engine.Execution;
using System.Linq;
using static SproutDB.Engine.Compilation.Expression;

namespace SproutDB.Engine.Tests;

[TestClass]
public sealed class ISproutConnectionTests
{
    // Constants for database, table, and column names
    private const string TEST_DATABASE = "testdb";
    private const string USERS_TABLE = "users";
    private const string OLD_TABLE = "oldtable";
    private const string NAME_COLUMN = "name";
    private const string AGE_COLUMN = "age";
    private const string ACTIVE_COLUMN = "active";
    private const string PROFILE_COLUMN = "profile";
    private const string SETTINGS_FIELD = "settings";
    private const string PREFERENCES_FIELD = "preferences";
    private const string THEME_FIELD = "theme";
    private const string NOTIFICATIONS_FIELD = "notifications";

    // Constants for column types
    private const string STRING_TYPE = "string";
    private const string NUMBER_TYPE = "number";
    private const string BOOLEAN_TYPE = "boolean";
    private const string MIXED_TYPE = "mixed";

    // Constants for user data
    private const string ALICE_NAME = "Alice Cooper";
    private const int ALICE_AGE = 42;
    private const bool ALICE_ACTIVE = true;
    private const int ALICE_UPDATED_AGE = 43;
    private const bool ALICE_UPDATED_ACTIVE = false;
    private const string BOB_NAME = "Bob Johnson";
    private const int BOB_AGE = 38;
    private const bool BOB_ACTIVE = true;
    private const string JOHN_NAME = "John Doe";
    private const int JOHN_AGE = 30;
    private const bool JOHN_ACTIVE = true;
    private const string JANE_NAME = "Jane Smith";
    private const int JANE_AGE = 25;
    private const bool JANE_ACTIVE = true;
    private const int BATCH_BOB_AGE = 40;
    private const bool BATCH_BOB_ACTIVE = false;

    // Constants for nested object test
    private const string ALICE_GREEN_NAME = "Alice Green";
    private const int ALICE_GREEN_AGE = 28;
    private const bool ALICE_GREEN_ACTIVE = true;
    private const string DARK_THEME = "dark";
    private const bool NOTIFICATIONS_ENABLED = true;
    private const string PREFERENCE_EMAIL = "email";
    private const string PREFERENCE_SMS = "sms";

    private ISproutConnection _connection = null!;
    private ISproutDB _server = null!;

    [TestInitialize]
    public void Setup()
    {
        var builder = Host.CreateApplicationBuilder();
        builder.AddSproutDB();
        var app = builder.Build();
        app.Start();
        _connection = app.Services.GetRequiredService<ISproutConnection>();
        _server = app.Services.GetRequiredService<ISproutDB>();
    }

    [TestMethod]
    public void Test_CreateDatabase_Works()
    {
        // Arrange
        var query = $"create database {TEST_DATABASE}";

        // Act
        var result = _connection.Execute(query);

        // Assert
        Assert.IsTrue(result.Success, $"Failed to create database: {result.Error}");
        Assert.IsNull(result.Error);
        Assert.IsTrue(_server.Databases.ContainsKey(TEST_DATABASE));
    }

    [TestMethod]
    public void Test_CreateTable_Works()
    {
        // Arrange
        var createDbResult = _connection.Execute($"create database {TEST_DATABASE}");
        Assert.IsTrue(createDbResult.Success, $"Failed to create database: {createDbResult.Error}");

        // Act
        var result = _connection.Execute($"create table {USERS_TABLE}");

        // Assert
        Assert.IsTrue(result.Success, $"Failed to create table: {result.Error}");
        Assert.IsNull(result.Error);
        var database = _server.Databases[TEST_DATABASE];
        Assert.IsTrue(database.Tables.ContainsKey(USERS_TABLE));
    }

    [TestMethod]
    public void Test_AddColumn_StringType_Works()
    {
        // Arrange
        var createDbResult = _connection.Execute($"create database {TEST_DATABASE}");
        Assert.IsTrue(createDbResult.Success, $"Failed to create database: {createDbResult.Error}");
        var createTableResult = _connection.Execute($"create table {USERS_TABLE}");
        Assert.IsTrue(createTableResult.Success, $"Failed to create table: {createTableResult.Error}");

        // Act
        var result = _connection.Execute($"add column {USERS_TABLE}.{NAME_COLUMN} {STRING_TYPE}");

        // Assert
        Assert.IsTrue(result.Success, $"Failed to add column: {result.Error}");
        Assert.IsNull(result.Error);
        var database = _server.Databases[TEST_DATABASE];
        var table = database.Tables[USERS_TABLE];
        Assert.IsTrue(table.Columns.ContainsKey(NAME_COLUMN));
    }

    [TestMethod]
    public void Test_AddColumn_NumberType_Works()
    {
        // Arrange
        var createDbResult = _connection.Execute($"create database {TEST_DATABASE}");
        Assert.IsTrue(createDbResult.Success);
        var createTableResult = _connection.Execute($"create table {USERS_TABLE}");
        Assert.IsTrue(createTableResult.Success);

        // Act
        var result = _connection.Execute($"add column {USERS_TABLE}.{AGE_COLUMN} {NUMBER_TYPE}");

        // Assert
        Assert.IsTrue(result.Success, $"Failed to add column: {result.Error}");
        Assert.IsNull(result.Error);
        var database = _server.Databases[TEST_DATABASE];
        var table = database.Tables[USERS_TABLE];
        Assert.IsTrue(table.Columns.ContainsKey(AGE_COLUMN));
    }

    [TestMethod]
    public void Test_WidenColumn_NumberToMixed_Works()
    {
        // Arrange
        var createDbResult = _connection.Execute($"create database {TEST_DATABASE}");
        Assert.IsTrue(createDbResult.Success);
        var createTableResult = _connection.Execute($"create table {USERS_TABLE}");
        Assert.IsTrue(createTableResult.Success);
        var addColumnResult = _connection.Execute($"add column {USERS_TABLE}.{AGE_COLUMN} {NUMBER_TYPE}");
        Assert.IsTrue(addColumnResult.Success);

        // Act
        var result = _connection.Execute($"add column {USERS_TABLE}.{AGE_COLUMN} {MIXED_TYPE}");

        // Assert
        Assert.IsTrue(result.Success, $"Failed to widen column: {result.Error}");
        Assert.IsNull(result.Error);
        var database = _server.Databases[TEST_DATABASE];
        var table = database.Tables[USERS_TABLE];
        Assert.IsTrue(table.Columns.ContainsKey(AGE_COLUMN));
        Assert.AreEqual(EColumnType.Mixed, table.Columns[AGE_COLUMN].Type);
    }

    [TestMethod]
    public void Test_AddColumn_BooleanType_Works()
    {
        // Arrange
        var createDbResult = _connection.Execute($"create database {TEST_DATABASE}");
        Assert.IsTrue(createDbResult.Success);
        var createTableResult = _connection.Execute($"create table {USERS_TABLE}");
        Assert.IsTrue(createTableResult.Success);

        // Act
        var result = _connection.Execute($"add column {USERS_TABLE}.{ACTIVE_COLUMN} {BOOLEAN_TYPE}");

        // Assert
        Assert.IsTrue(result.Success, $"Failed to add column: {result.Error}");
        Assert.IsNull(result.Error);
        var database = _server.Databases[TEST_DATABASE];
        var table = database.Tables[USERS_TABLE];
        Assert.IsTrue(table.Columns.ContainsKey(ACTIVE_COLUMN));
    }

    [TestMethod]
    public void Test_DropTable_Works()
    {
        // Arrange
        var createDbResult = _connection.Execute($"create database {TEST_DATABASE}");
        Assert.IsTrue(createDbResult.Success);
        var createTableResult = _connection.Execute($"create table {OLD_TABLE}");
        Assert.IsTrue(createTableResult.Success);

        // Act
        var result = _connection.Execute($"drop table {OLD_TABLE}");

        // Assert
        Assert.IsTrue(result.Success, $"Failed to drop table: {result.Error}");
        Assert.IsNull(result.Error);
        var database = _server.Databases[TEST_DATABASE];
        Assert.IsFalse(database.Tables.ContainsKey(OLD_TABLE));
    }

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
    public void Test_Delete_WithWhereClause_Works()
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
        var aliceUpsertResult = _connection.Execute($"upsert {USERS_TABLE} {{ {NAME_COLUMN}: '{ALICE_NAME}', {AGE_COLUMN}: {ALICE_AGE}, {ACTIVE_COLUMN}: {ALICE_ACTIVE.ToString().ToLower()} }}");
        Assert.IsTrue(aliceUpsertResult.Success);
        var bobUpsertResult = _connection.Execute($"upsert {USERS_TABLE} {{ {NAME_COLUMN}: '{BOB_NAME}', {AGE_COLUMN}: {BOB_AGE}, {ACTIVE_COLUMN}: {BOB_ACTIVE.ToString().ToLower()} }}");
        Assert.IsTrue(bobUpsertResult.Success);

        // Act
        var result = _connection.Execute($"delete {USERS_TABLE} where {NAME_COLUMN} = '{BOB_NAME}'");

        // Assert
        Assert.IsTrue(result.Success, $"Failed to delete record: {result.Error}");
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



get users where age > 25
get users where age >= 30
get users where age < 40
get users where age <= 35
get users where name = 'John Doe'
get users where name != 'Jane Smith'
get users where age > 25 and active = true
get users where name = 'John' or name = 'Jane'
get users where not active = false
get users where name in ['John', 'Jane', 'Bob']
get users where name contains 'oh'


count users
count users where age > 30
sum users.age
sum users.age where active = true
avg users.age
avg users.age where name contains 'J'



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


get orders where date last 7 days
get orders where date this month
get orders where date before '2024-08-01'
get orders where date after '2024-07-15'
get orders where date > '2024-07-01' and date < '2024-08-01'


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
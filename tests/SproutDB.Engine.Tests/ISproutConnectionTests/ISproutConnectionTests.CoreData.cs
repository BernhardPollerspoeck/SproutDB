namespace SproutDB.Engine.Tests.ISproutConnectionTests;

public abstract class ISproutConnectionTestsCoreData
{
	// Constants for database, table, and column names
	protected const string TEST_DATABASE = "testdb";
	protected const string USERS_TABLE = "users";
	protected const string ORDERS_TABLE = "orders";
	protected const string OLD_TABLE = "oldtable";
	protected const string NAME_COLUMN = "name";
	protected const string AGE_COLUMN = "age";
	protected const string ACTIVE_COLUMN = "active";
	protected const string DATE_COLUMN = "date";
	protected const string TOTAL_COLUMN = "total";
	protected const string USER_ID_COLUMN = "user_id";
	protected const string STATUS_COLUMN = "status";
	protected const string PROFILE_COLUMN = "profile";
	protected const string SETTINGS_FIELD = "settings";
	protected const string PREFERENCES_FIELD = "preferences";
	protected const string THEME_FIELD = "theme";
	protected const string NOTIFICATIONS_FIELD = "notifications";

	// Constants for column types
	protected const string STRING_TYPE = "string";
	protected const string NUMBER_TYPE = "number";
	protected const string BOOLEAN_TYPE = "boolean";
	protected const string MIXED_TYPE = "mixed";

	// Constants for user data
	protected const string ALICE_NAME = "Alice Cooper";
	protected const double ALICE_AGE = 42;
	protected const bool ALICE_ACTIVE = true;
	protected const double ALICE_UPDATED_AGE = 43;
	protected const bool ALICE_UPDATED_ACTIVE = false;
	protected const string BOB_NAME = "Bob Johnson";
	protected const double BOB_AGE = 38;
	protected const bool BOB_ACTIVE = true;
	protected const string JOHN_NAME = "John Doe";
	protected const double JOHN_AGE = 30;
	protected const bool JOHN_ACTIVE = true;
	protected const string JANE_NAME = "Jane Smith";
	protected const double JANE_AGE = 25;
	protected const bool JANE_ACTIVE = true;
	protected const double BATCH_BOB_AGE = 40;
	protected const bool BATCH_BOB_ACTIVE = false;

	// Constants for nested object test
	protected const string ALICE_GREEN_NAME = "Alice Green";
	protected const double ALICE_GREEN_AGE = 28;
	protected const bool ALICE_GREEN_ACTIVE = true;
	protected const string DARK_THEME = "dark";
	protected const bool NOTIFICATIONS_ENABLED = true;
	protected const string PREFERENCE_EMAIL = "email";
	protected const string PREFERENCE_SMS = "sms";


}

namespace SproutDB.Core.Parsing;

internal static class ErrorMessages
{
    public const string EXPECTED_COMMAND =
        "expected a command";

    public const string UNKNOWN_COMMAND =
        "unknown command, expected 'create', 'get', 'upsert', 'delete', 'add', 'describe', 'purge', 'rename' or 'alter'";

    public const string EXPECTED_CREATE_TARGET =
        "expected 'database', 'table' or 'index'";

    public const string UNEXPECTED_TOKEN_EXPECTED_EOF =
        "unexpected token, expected end of query";

    public const string EXPECTED_TABLE_NAME =
        "expected table name";

    public const string EXPECTED_COLUMN_NAME =
        "expected column name";

    public const string EXPECTED_COLUMN_TYPE =
        "expected column type";

    public const string EXPECTED_COMMA_OR_CLOSE_PAREN =
        "expected ',' or ')'";

    public const string EXPECTED_DEFAULT_VALUE =
        "expected default value";

    public const string RESERVED_COLUMN_NAME_ID =
        "column name 'id' is reserved";

    public const string EXPECTED_OPEN_BRACE =
        "expected '{'";

    public const string EXPECTED_COMMA_OR_CLOSE_BRACE =
        "expected ',' or '}'";

    public const string EXPECTED_FIELD_NAME =
        "expected field name";

    public const string EXPECTED_COLON =
        "expected ':'";

    public const string EXPECTED_VALUE =
        "expected a value";

    public const string EXPECTED_COLUMN_KEYWORD =
        "expected 'column'";

    public const string EXPECTED_DOT =
        "expected '.'";
}

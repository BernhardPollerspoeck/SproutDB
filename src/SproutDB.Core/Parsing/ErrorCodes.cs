namespace SproutDB.Core.Parsing;

internal static class ErrorCodes
{
    public const string SYNTAX_ERROR = "SYNTAX_ERROR";
    public const string UNKNOWN_TABLE = "UNKNOWN_TABLE";
    public const string UNKNOWN_COLUMN = "UNKNOWN_COLUMN";
    public const string TABLE_EXISTS = "TABLE_EXISTS";
    public const string DATABASE_EXISTS = "DATABASE_EXISTS";
    public const string TYPE_MISMATCH = "TYPE_MISMATCH";
    public const string NOT_NULLABLE = "NOT_NULLABLE";
    public const string TYPE_NARROWING = "TYPE_NARROWING";
    public const string STRICT_VIOLATION = "STRICT_VIOLATION";
    public const string UNKNOWN_DATABASE = "UNKNOWN_DATABASE";
    public const string BULK_LIMIT = "BULK_LIMIT";
    public const string WHERE_REQUIRED = "WHERE_REQUIRED";
}

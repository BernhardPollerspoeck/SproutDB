namespace SproutDB.Engine.Parsing;

public enum TokenType
{
    // End of input
    EOF,

    // Literals
    Identifier,          // table names, field names, aliases
    String,              // "text in quotes"
    Number,              // 123, 123.45
    Boolean,             // true, false
    Date,                // "2024-01-01 12:30:45.1234"

    // Keywords - Query Operations
    Get,                 // get
    Upsert,              // upsert
    Delete,              // delete
    Count,               // count
    Sum,                 // sum
    Avg,                 // avg

    // Keywords - Query Clauses
    Follow,              // follow
    Where,               // where
    Group,               // group
    By,                  // by (shared between group by, order by)
    Having,              // having
    Order,               // order
    Select,              // select
    As,                  // as
    On,                  // on
    For,                 // for

    // Keywords - Pagination
    Page,                // page
    Of,                  // of
    Size,                // size

    // Keywords - Join Types
    Left,                // left
    Inner,               // inner
    Right,               // right

    // Keywords - Schema Operations
    Create,              // create
    Drop,                // drop
    Add,                 // add
    Purge,               // purge
    Table,               // table
    Column,              // column
    Database,            // database

    // Keywords - Branch Operations
    Branch,              // branch
    Commit,              // commit
    Checkout,            // checkout
    Merge,               // merge
    Into,                // into
    From,                // from
    Alias,               // alias
    Update,              // update
    Protect,             // protect
    Unprotect,           // unprotect
    Abandon,             // abandon
    Reactivate,          // reactivate

    // Keywords - Auth Operations
    Auth,                // auth
    Token,               // token
    With,                // with
    Disable,             // disable
    Enable,              // enable
    Revoke,              // revoke
    List,                // list

    // Keywords - Meta Operations
    Backup,              // backup
    Restore,             // restore
    Explain,             // explain
    Respawn,             // respawn
    Since,               // since

    // Keywords - Time/Date
    Last,                // last
    This,                // this
    Days,                // days
    Hours,               // hours
    Minutes,             // minutes
    Weeks,               // weeks
    Month,               // month
    Year,                // year
    Before,              // before
    After,               // after
    Ago,                 // ago

    // Operators
    Equals,              // =
    NotEquals,           // !=, <>
    LessThan,            // <
    LessThanOrEqual,     // <=
    GreaterThan,         // >
    GreaterThanOrEqual,  // >=

    // Logical Operators
    And,                 // and
    Or,                  // or
    Not,                 // not

    // Collection Operators
    In,                  // in
    Contains,            // contains
    Any,                 // any

    // Punctuation
    Dot,                 // .
    Comma,               // ,
    LeftParen,           // (
    RightParen,          // )
    LeftBracket,         // [
    RightBracket,        // ]
    LeftBrace,           // {
    RightBrace,          // }
    Arrow,               // ->
    Colon,               // :
    Semicolon,           // ;

    // Sorting
    Asc,                 // asc
    Desc,                // desc

    // Special Values
    Null,                // null

    // Error token for invalid input
    Invalid
}
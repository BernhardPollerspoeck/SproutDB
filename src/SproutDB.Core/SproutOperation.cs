namespace SproutDB.Core;

/// <summary>
/// Identifies the operation that was executed or attempted.
/// </summary>
public enum SproutOperation : byte
{
    Error = 0,
    Get = 1,
    Upsert = 2,
    Delete = 3,
    Describe = 4,
    CreateTable = 5,
    CreateDatabase = 6,
    PurgeTable = 7,
    PurgeDatabase = 8,
    PurgeColumn = 9,
    AddColumn = 10,
    RenameColumn = 11,
    AlterColumn = 12,
    CreateIndex = 13,
    PurgeIndex = 14,
    Backup = 15,
    Restore = 16,
    CreateApiKey = 17,
    PurgeApiKey = 18,
    RotateApiKey = 19,
    Grant = 20,
    Revoke = 21,
    Restrict = 22,
    Unrestrict = 23,
    PurgeTtl = 24,
    ShrinkTable = 25,
    ShrinkDatabase = 26,
}

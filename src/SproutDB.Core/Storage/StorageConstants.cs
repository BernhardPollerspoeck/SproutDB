namespace SproutDB.Core.Storage;

internal static class StorageConstants
{
    public const int CHUNK_SIZE = 10_000;
    public const int INDEX_ENTRY_SIZE = sizeof(long); // 8 bytes per slot
    public const double BACKFILL_THRESHOLD = 0.2; // 20% free → backfill instead of grow
    public const byte FLAG_NULL = 0x00;
    public const byte FLAG_VALUE = 0x01;
}

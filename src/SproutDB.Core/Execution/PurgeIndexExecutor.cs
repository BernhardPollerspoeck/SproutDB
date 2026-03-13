using SproutDB.Core.Parsing;
using SproutDB.Core.Storage;

namespace SproutDB.Core.Execution;

internal static class PurgeIndexExecutor
{
    public static SproutResponse Execute(string query, TableHandle table, PurgeIndexQuery q)
    {
        // Column must exist
        if (!table.HasColumn(q.Column))
            return ResponseHelper.Error(query, ErrorCodes.UNKNOWN_COLUMN,
                $"column '{q.Column}' does not exist");

        // Index must exist
        if (!table.HasBTree(q.Column))
            return ResponseHelper.Error(query, ErrorCodes.INDEX_NOT_FOUND,
                $"index on '{q.Column}' does not exist");

        // Clear unique flag if set
        var colSchema = table.Schema.Columns.Find(c => c.Name == q.Column);
        if (colSchema is not null && colSchema.IsUnique)
        {
            colSchema.IsUnique = false;
            table.SaveSchema();
        }

        table.RemoveBTree(q.Column);

        return new SproutResponse
        {
            Operation = SproutOperation.PurgeIndex,
            Affected = 1,
        };
    }
}

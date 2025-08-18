using SproutDB.Engine.Compilation;
using SproutDB.Engine.Core;

namespace SproutDB.Engine.Execution;

// Enhanced data interfaces for production
public interface IDataStore
{
    // Table operations
    IEnumerable<Row> GetRows(string tableName, string? alias, int? limit = null);
    Row? GetRow(string tableName, object id);
    string UpsertRow(string tableName, Row row, string? onField = null);
    int DeleteRows(string tableName, Expression? filter = null);
    int CountRows(string tableName, Expression? filter = null);

    // Schema operations
    void CreateTable(string tableName);
    void DropTable(string tableName);
    void AddColumn(string tableName, string columnName, EColumnType type);
    void PurgeColumn(string tableName, string columnName);

}

namespace SproutDB.Engine.Compilation;

public readonly struct SchemaStatement(
    int position, 
    SchemaOperation operation, 
    string? databaseName = null, 
    string? tableName = null,
    string? columnName = null, 
    string? dataType = null) 
    : IStatement
{
    public StatementType Type => StatementType.Schema;
    public int Position { get; } = position;
    public SchemaOperation Operation { get; } = operation;
    public string? DatabaseName { get; } = databaseName;
    public string? TableName { get; } = tableName;
    public string? ColumnName { get; } = columnName;
    public string? DataType { get; } = dataType;
}


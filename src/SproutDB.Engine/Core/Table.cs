namespace SproutDB.Engine.Core;

public class Table
{
    public IDictionary<string, Column> Columns { get; } = new Dictionary<string, Column>();
    public IDictionary<object, Row> Rows { get; } = new Dictionary<object, Row>();
}

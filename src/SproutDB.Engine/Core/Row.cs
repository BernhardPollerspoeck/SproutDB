namespace SproutDB.Engine.Core;

public class Row
{
    public object Id { get; set; } = null!;
    public Dictionary<string, object?> Fields { get; set; } = new();

    public T? GetField<T>(string fieldName)
    {
        return Fields.TryGetValue(fieldName, out var value) && value is T typed ? typed : default;
    }

    public void SetField(string fieldName, object? value)
    {
        Fields[fieldName] = value;
    }
}
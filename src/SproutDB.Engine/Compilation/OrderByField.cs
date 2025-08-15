namespace SproutDB.Engine.Compilation;

public readonly struct OrderByField(Expression field, SortDirection direction = SortDirection.Asc)
{
    public Expression Field { get; } = field;
    public SortDirection Direction { get; } = direction;
}


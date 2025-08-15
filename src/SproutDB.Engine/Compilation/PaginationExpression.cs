namespace SproutDB.Engine.Compilation;

public readonly struct PaginationExpression(int position, int page, int size)
{
    public int Position { get; } = position;
    public int Page { get; } = page;
    public int Size { get; } = size;
}


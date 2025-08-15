namespace SproutDB.Engine.Compilation;

public class ParseException : Exception
{
    public int Position { get; }

    public ParseException(string message, int position) : base($"{message} at position {position}")
    {
        Position = position;
    }

    public ParseException(string message, int position, Exception innerException)
        : base($"{message} at position {position}", innerException)
    {
        Position = position;
    }
}


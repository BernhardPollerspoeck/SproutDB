namespace SproutDB.Core;

public sealed class SproutQueryException : Exception
{
    public SproutQueryException(string message) : base(message) { }
}

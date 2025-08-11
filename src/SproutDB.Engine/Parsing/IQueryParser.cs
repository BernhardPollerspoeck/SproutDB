namespace SproutDB.Engine.Parsing;

public interface IQueryParser
{
    IEnumerable<Token> Parse(string query);
}



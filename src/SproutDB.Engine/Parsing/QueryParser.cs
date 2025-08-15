namespace SproutDB.Engine.Parsing;

internal class QueryParser : IQueryParser
{
    public IEnumerable<Token> Parse(string query)
    {
        var tokenizer = new Tokenizer(query);
        foreach (var token in tokenizer)
        {
            yield return token;
        }
    }

}



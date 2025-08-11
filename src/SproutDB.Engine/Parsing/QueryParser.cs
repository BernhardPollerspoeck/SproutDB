namespace SproutDB.Engine.Parsing;

internal class QueryParser : IQueryParser
{
    public IEnumerable<Token> Parse(string query)
    {
        if (string.IsNullOrWhiteSpace(query))
        {
            yield break;
        }

        // Split the query into individual words
        //TODO: improve with char crawling and a state machine
        var words = query.Split([' ', '\t', '\n', '\r'], StringSplitOptions.RemoveEmptyEntries);

        // Create a token for each word
        foreach (var word in words)
        {
            var tokenType = DetermineTokenType(word.ToLowerInvariant());
            yield return new Token(word, tokenType);
        }
    }

    private static TokenType DetermineTokenType(string word)
    {
        return word.ToLowerInvariant() switch
        {
            "create" => TokenType.Create,
            "table" => TokenType.Table,
            "database" => TokenType.Database,
            "column" => TokenType.Column,
            //"select" => TokenType.Select,
            //"from" => TokenType.From,
            // "where" => TokenType.Where,
            //"insert" => TokenType.Insert,
            //"into" => TokenType.Into,
            //"update" => TokenType.Update,
            //"delete" => TokenType.Delete,
            _ => TokenType.Identifier
        };
    }
}



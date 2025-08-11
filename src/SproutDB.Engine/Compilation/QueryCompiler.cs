using SproutDB.Engine.Parsing;

namespace SproutDB.Engine.Compilation;

internal class QueryCompiler : IQueryCompiler
{
    public Node Compile(IEnumerable<Token> tokens)
    {
        var tokenList = tokens.ToList();
        if (tokenList.Count == 0)
        {
            throw new SyntaxException("Empty token list");
        }

        return tokenList[0].Type switch
        {
            TokenType.Create => CompileCreateStatement(tokenList),
            _ => throw new SyntaxException($"Unexpected token type: {tokenList[0].Type}")
        };
    }

    private Node CompileCreateStatement(List<Token> tokens)
    {
        if (tokens.Count < 3)
        {
            throw new SyntaxException("Invalid CREATE statement");
        }

        if (tokens[2].Type != TokenType.Identifier)
        {
            throw new SyntaxException("Expected table name identifier");
        }

        var createType = tokens[1].Type switch
        {
            TokenType.Table => ECreateType.Table,
            TokenType.Database => ECreateType.Database,
            _ => throw new SyntaxException($"Unsupported create type: {tokens[1].Type}")
        };

        var tableName = tokens[2].Value;
        var identifierNode = new IdentifierNode(tableName);
        return new CreateNode("CREATE TABLE", createType, identifierNode);
    }
}



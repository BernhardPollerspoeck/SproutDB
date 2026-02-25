namespace SproutDB.Core.Parsing;

internal sealed class ParseResult
{
    public IQuery? Query { get; }
    public IReadOnlyList<ParseError>? Errors { get; }
    public string? AnnotatedQuery { get; }
    public bool Success => Query is not null;

    private ParseResult(IQuery query)
    {
        Query = query;
    }

    private ParseResult(List<ParseError> errors, string annotatedQuery)
    {
        Errors = errors;
        AnnotatedQuery = annotatedQuery;
    }

    public static ParseResult Ok(IQuery query) => new(query);
    public static ParseResult Fail(List<ParseError> errors, string annotatedQuery) => new(errors, annotatedQuery);
}

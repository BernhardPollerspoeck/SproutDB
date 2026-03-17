namespace SproutDB.Core.Parsing;

internal sealed class ParseResult
{
    public IQuery? Query { get; }
    public IReadOnlyList<ParseError>? Errors { get; }
    public string? AnnotatedQuery { get; }
    public bool Success => Query is not null;

    /// <summary>
    /// The original text segment for this query (set by ParseMulti for multi-query inputs).
    /// Null when parsed via single Parse().
    /// </summary>
    public string? OriginalText { get; set; }

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

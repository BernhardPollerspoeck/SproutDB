namespace SproutDB.Core.Parsing;

internal static class CreateDatabaseParser
{
    public static ParseResult Parse(ParserContext ctx)
    {
        ctx.ExpectEof();
        return ctx.HasErrors
            ? ctx.Fail()
            : ParseResult.Ok(new CreateDatabaseQuery());
    }
}

namespace SproutDB.Core.Parsing;

internal static class BackupParser
{
    public static ParseResult Parse(ParserContext ctx)
    {
        ctx.ExpectEof();
        if (ctx.HasErrors) return ctx.Fail();

        return ParseResult.Ok(new BackupQuery());
    }
}

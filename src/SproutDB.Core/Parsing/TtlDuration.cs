namespace SproutDB.Core.Parsing;

/// <summary>
/// Parses TTL duration strings like "24h", "7d", "30m" into seconds.
/// Units: m (minutes), h (hours), d (days).
/// </summary>
internal static class TtlDuration
{
    /// <summary>
    /// Parses a TTL duration from two consecutive tokens: integer + identifier (m/h/d).
    /// Returns duration in seconds, or -1 on error.
    /// </summary>
    public static long ParseFromTokens(ParserContext ctx)
    {
        var numToken = ctx.Peek();
        if (numToken.Type != TokenType.IntegerLiteral)
        {
            ctx.AddError(numToken, ErrorCodes.SYNTAX_ERROR, "expected TTL duration (e.g. 24h, 7d, 30m)");
            return -1;
        }
        var amount = long.Parse(ctx.GetText(numToken));
        ctx.Advance();

        var unitToken = ctx.Peek();
        if (unitToken.Type != TokenType.Identifier)
        {
            ctx.AddError(unitToken, ErrorCodes.SYNTAX_ERROR, "expected TTL unit: m (minutes), h (hours), d (days)");
            return -1;
        }

        var unit = ctx.GetLowercaseText(unitToken);
        ctx.Advance();

        return unit switch
        {
            "m" => amount * 60,
            "h" => amount * 3600,
            "d" => amount * 86400,
            _ => Error(ctx, unitToken),
        };
    }

    private static long Error(ParserContext ctx, Token token)
    {
        ctx.AddError(token, ErrorCodes.SYNTAX_ERROR, "invalid TTL unit, expected m (minutes), h (hours), d (days)");
        return -1;
    }
}

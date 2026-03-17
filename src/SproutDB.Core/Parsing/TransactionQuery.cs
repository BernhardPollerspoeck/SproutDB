namespace SproutDB.Core.Parsing;

internal sealed class TransactionQuery : IQuery
{
    public SproutOperation Operation => SproutOperation.Transaction;
    public required List<IQuery> Queries { get; init; }
    public required List<string> QueryTexts { get; init; }
}

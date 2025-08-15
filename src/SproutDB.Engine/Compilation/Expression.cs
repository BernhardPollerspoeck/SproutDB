namespace SproutDB.Engine.Compilation;

public readonly struct Expression
{
    public ExpressionType Type { get; }
    public int Position { get; }
    private readonly object _value;

    private Expression(ExpressionType type, int position, object value)
    {
        Type = type;
        Position = position;
        _value = value;
    }

    public T As<T>() => (T)_value;

    public static Expression FieldPath(int position, ReadOnlyMemory<string> segments)
        => new(ExpressionType.FieldPath, position, segments);

    public static Expression Binary(int position, LogicalOperator op, Expression left, Expression right)
        => new(ExpressionType.Binary, position, new BinaryData(op, left, right));

    public static Expression Comparison(int position, ComparisonOperator op, Expression left, Expression right)
        => new(ExpressionType.Comparison, position, new ComparisonData(op, left, right));

    public static Expression Literal(int position, LiteralType literalType, string value)
        => new(ExpressionType.Literal, position, new LiteralData(literalType, value));

    public static Expression JsonValue(int position, JsonValueType valueType, object value)
        => new(ExpressionType.JsonValue, position, new JsonData(valueType, value));

    // Helper structs for typed data
    public readonly struct BinaryData
    {
        public LogicalOperator Operator { get; }
        public Expression Left { get; }
        public Expression Right { get; }

        public BinaryData(LogicalOperator op, Expression left, Expression right)
        {
            Operator = op;
            Left = left;
            Right = right;
        }
    }

    public readonly struct ComparisonData
    {
        public ComparisonOperator Operator { get; }
        public Expression Left { get; }
        public Expression Right { get; }

        public ComparisonData(ComparisonOperator op, Expression left, Expression right)
        {
            Operator = op;
            Left = left;
            Right = right;
        }
    }

    public readonly struct LiteralData
    {
        public LiteralType LiteralType { get; }
        public string Value { get; }

        public LiteralData(LiteralType literalType, string value)
        {
            LiteralType = literalType;
            Value = value;
        }
    }

    public readonly struct JsonData
    {
        public JsonValueType ValueType { get; }
        public object Value { get; }

        public JsonData(JsonValueType valueType, object value)
        {
            ValueType = valueType;
            Value = value;
        }
    }
}


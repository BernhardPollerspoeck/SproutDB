using System.Text.Json;
using System.Text.Json.Serialization;

namespace SproutDB.Core.Client;

/// <summary>
/// JSON settings for reading server responses. Row values arrive as plain JSON and
/// are mapped to CLR values: string, long (ulong if larger), double, bool, null,
/// List&lt;object?&gt; for arrays — and <c>_id</c> always as <see cref="ulong"/>, like
/// the embedded engine.
/// </summary>
internal static class SproutClientJson
{
    /// <summary>For the HTTP endpoint (snake_case, like the server).</summary>
    public static readonly JsonSerializerOptions Http = Create(JsonNamingPolicy.SnakeCaseLower);

    public static JsonSerializerOptions Create(JsonNamingPolicy namingPolicy)
    {
        var options = new JsonSerializerOptions
        {
            PropertyNamingPolicy = namingPolicy,
            PropertyNameCaseInsensitive = true,
        };
        options.Converters.Add(new RowConverter());
        return options;
    }

    /// <summary>
    /// Adds the row converter to existing options (e.g. the SignalR payload options).
    /// </summary>
    public static void AddRowConverter(JsonSerializerOptions options) => options.Converters.Add(new RowConverter());

    private sealed class RowConverter : JsonConverter<Dictionary<string, object?>>
    {
        public override Dictionary<string, object?> Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
        {
            using var doc = JsonDocument.ParseValue(ref reader);
            var row = new Dictionary<string, object?>();
            if (doc.RootElement.ValueKind != JsonValueKind.Object)
                return row;

            foreach (var prop in doc.RootElement.EnumerateObject())
            {
                row[prop.Name] = prop.Name == "_id" && prop.Value.TryGetUInt64(out var id)
                    ? id
                    : ToClr(prop.Value);
            }
            return row;
        }

        public override void Write(Utf8JsonWriter writer, Dictionary<string, object?> value, JsonSerializerOptions options)
        {
            writer.WriteStartObject();
            foreach (var (key, item) in value)
            {
                writer.WritePropertyName(key);
                JsonSerializer.Serialize(writer, item, options);
            }
            writer.WriteEndObject();
        }

        private static object? ToClr(JsonElement element)
        {
            switch (element.ValueKind)
            {
                case JsonValueKind.String:
                    return element.GetString();
                case JsonValueKind.True:
                    return true;
                case JsonValueKind.False:
                    return false;
                case JsonValueKind.Number:
                    if (element.TryGetInt64(out var l)) return l;
                    if (element.TryGetUInt64(out var ul)) return ul;
                    return element.GetDouble();
                case JsonValueKind.Array:
                    var list = new List<object?>();
                    foreach (var item in element.EnumerateArray())
                        list.Add(ToClr(item));
                    return list;
                default:
                    return null;
            }
        }
    }
}

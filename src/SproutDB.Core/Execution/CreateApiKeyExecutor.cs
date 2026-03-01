using SproutDB.Core.Auth;
using SproutDB.Core.Parsing;
using SproutDB.Core.Storage;

namespace SproutDB.Core.Execution;

internal static class CreateApiKeyExecutor
{
    public static SproutResponse Execute(
        string query,
        CreateApiKeyQuery q,
        TableHandle apiKeysTable,
        SproutAuthService authService,
        int bulkLimit)
    {
        if (authService.KeyExists(q.Name))
            return ResponseHelper.Error(query, ErrorCodes.KEY_EXISTS,
                $"api key '{q.Name}' already exists");

        var apiKey = ApiKeyGenerator.Generate();
        var hash = ApiKeyGenerator.Hash(apiKey);
        var prefix = ApiKeyGenerator.ExtractPrefix(apiKey);
        var now = DateTime.UtcNow;
        var nowStr = now.ToString("yyyy-MM-dd HH:mm:ss");

        var upsertQuery = $"upsert _api_keys {{name: '{Escape(q.Name)}', key_prefix: '{Escape(prefix)}', key_hash: '{Escape(hash)}', created_at: '{nowStr}'}}";
        var parseResult = QueryParser.Parse(upsertQuery);
        if (!parseResult.Success || parseResult.Query is not UpsertQuery uq)
            return ResponseHelper.Error(query, ErrorCodes.SYNTAX_ERROR, "internal error creating api key");

        var result = UpsertExecutor.Execute(upsertQuery, apiKeysTable, uq, bulkLimit);
        if (result.Errors is not null)
            return result;

        authService.OnKeyCreated(q.Name, prefix, hash);

        return new SproutResponse
        {
            Operation = SproutOperation.CreateApiKey,
            Data =
            [
                new Dictionary<string, object?>
                {
                    ["name"] = q.Name,
                    ["api_key"] = apiKey,
                    ["key_prefix"] = prefix,
                    ["created_at"] = now,
                }
            ],
            Affected = 1,
        };
    }

    private static string Escape(string value) => value.Replace("'", "\\'");
}

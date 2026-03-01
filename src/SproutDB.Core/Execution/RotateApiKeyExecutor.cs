using SproutDB.Core.Auth;
using SproutDB.Core.Parsing;
using SproutDB.Core.Storage;

namespace SproutDB.Core.Execution;

internal static class RotateApiKeyExecutor
{
    public static SproutResponse Execute(
        string query,
        RotateApiKeyQuery q,
        TableHandle apiKeysTable,
        SproutAuthService authService,
        int bulkLimit)
    {
        var existing = authService.GetKeyByName(q.Name);
        if (existing is null)
            return ResponseHelper.Error(query, ErrorCodes.KEY_NOT_FOUND,
                $"api key '{q.Name}' not found");

        var newApiKey = ApiKeyGenerator.Generate();
        var newHash = ApiKeyGenerator.Hash(newApiKey);
        var newPrefix = ApiKeyGenerator.ExtractPrefix(newApiKey);
        var now = DateTime.UtcNow;
        var nowStr = now.ToString("yyyy-MM-dd HH:mm:ss");

        // Update key_hash and key_prefix via upsert on name
        var upsertQuery = $"upsert _api_keys {{name: '{Escape(q.Name)}', key_prefix: '{Escape(newPrefix)}', key_hash: '{Escape(newHash)}', created_at: '{existing.CreatedAt:yyyy-MM-dd HH:mm:ss}'}} on name";
        var parseResult = QueryParser.Parse(upsertQuery);
        if (!parseResult.Success || parseResult.Query is not UpsertQuery uq)
            return ResponseHelper.Error(query, ErrorCodes.SYNTAX_ERROR, "internal error rotating api key");

        var result = UpsertExecutor.Execute(upsertQuery, apiKeysTable, uq, bulkLimit);
        if (result.Errors is not null)
            return result;

        authService.OnKeyRotated(q.Name, newPrefix, newHash);

        return new SproutResponse
        {
            Operation = SproutOperation.RotateApiKey,
            Data =
            [
                new Dictionary<string, object?>
                {
                    ["name"] = q.Name,
                    ["api_key"] = newApiKey,
                    ["key_prefix"] = newPrefix,
                    ["rotated_at"] = now,
                }
            ],
            Affected = 1,
        };
    }

    private static string Escape(string value) => value.Replace("'", "\\'");
}

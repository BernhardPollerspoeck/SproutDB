using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.SignalR;
using SproutDB.Core.Auth;

namespace SproutDB.Core.Server;

/// <summary>
/// SignalR hub for real-time change notifications.
/// Clients subscribe to <c>{database}.{table}</c> groups and receive <c>OnChange</c> events.
/// </summary>
public sealed class SproutChangeHub : Hub
{
    private const string ApiKeyItemKey = "SproutDB.ApiKey";

    private readonly SproutEngine _engine;

    public SproutChangeHub(SproutEngine engine)
    {
        _engine = engine;
    }

    public override Task OnConnectedAsync()
    {
        var authService = _engine.AuthService;
        if (authService is null)
            return base.OnConnectedAsync();

        // Extract API key from header or query string
        var httpContext = Context.GetHttpContext();
        var apiKey = GetApiKey(httpContext);

        if (string.IsNullOrWhiteSpace(apiKey))
        {
            Context.Abort();
            return Task.CompletedTask;
        }

        if (authService.IsMasterKey(apiKey))
        {
            // Master key — full access, store sentinel
            Context.Items[ApiKeyItemKey] = "master";
            return base.OnConnectedAsync();
        }

        var keyEntry = authService.ValidateKey(apiKey);
        if (keyEntry is null)
        {
            Context.Abort();
            return Task.CompletedTask;
        }

        Context.Items[ApiKeyItemKey] = keyEntry;
        return base.OnConnectedAsync();
    }

    public async Task Subscribe(string database, string table)
    {
        if (!CheckPermission(database))
            return;

        await Groups.AddToGroupAsync(Context.ConnectionId, $"{database}.{table}");
    }

    public async Task Unsubscribe(string database, string table)
    {
        await Groups.RemoveFromGroupAsync(Context.ConnectionId, $"{database}.{table}");
    }

    private bool CheckPermission(string database)
    {
        var authService = _engine.AuthService;
        if (authService is null)
            return true; // No auth configured — everything allowed

        if (!Context.Items.TryGetValue(ApiKeyItemKey, out var stored))
            return false;

        // Master key has full access
        if (stored is string s && s == "master")
            return true;

        if (stored is not ApiKeyEntry keyEntry)
            return false;

        // Key must have at least reader permission on the database
        return keyEntry.Permissions.ContainsKey(database);
    }

    private static string? GetApiKey(HttpContext? httpContext)
    {
        if (httpContext is null)
            return null;

        // Try header first
        var headerValue = httpContext.Request.Headers["X-SproutDB-ApiKey"].ToString();
        if (!string.IsNullOrWhiteSpace(headerValue))
            return headerValue;

        // Fall back to query string (for WebSocket connections that can't set headers)
        return httpContext.Request.Query["apiKey"].ToString();
    }
}

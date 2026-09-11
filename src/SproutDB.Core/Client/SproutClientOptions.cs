using Microsoft.AspNetCore.Http.Connections.Client;

namespace SproutDB.Core.Client;

/// <summary>
/// Settings for <see cref="SproutClient"/> — a SproutDB server reached over HTTP.
/// </summary>
public sealed class SproutClientOptions
{
    /// <summary>
    /// Base address of the SproutDB server (where <c>MapSproutDB()</c> is mapped),
    /// e.g. <c>https://db.example.com/</c>.
    /// </summary>
    public Uri? BaseAddress { get; set; }

    /// <summary>
    /// API key sent as <c>X-SproutDB-ApiKey</c> (only needed when the server has auth enabled).
    /// </summary>
    public string? ApiKey { get; set; }

    /// <summary>
    /// HTTP request timeout. Default: 100 seconds.
    /// </summary>
    public TimeSpan Timeout { get; set; } = TimeSpan.FromSeconds(100);

    /// <summary>
    /// Optional tweaks for the SignalR connections used by <c>OnChange</c>
    /// (transports, message handler, …).
    /// </summary>
    public Action<HttpConnectionOptions>? ConfigureChangeConnection { get; set; }
}

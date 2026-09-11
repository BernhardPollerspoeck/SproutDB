using System.Net;
using System.Text.Json;
using Microsoft.AspNetCore.SignalR.Client;
using SproutDB.Core.Execution;
using SproutDB.Core.Parsing;

namespace SproutDB.Core.Client;

/// <summary>
/// <see cref="ISproutDatabase"/> backed by a SproutDB server over HTTP.
/// Queries (incl. parameters and the typed API, which bind client-side) are sent to
/// <c>POST /sproutdb/query</c>; <see cref="OnChange"/> uses the SignalR hub.
/// </summary>
internal sealed class SproutRemoteDatabase : ISproutDatabase
{
    private readonly SproutClient _client;

    public string Name { get; }

    public SproutRemoteDatabase(SproutClient client, string name)
    {
        _client = client;
        Name = name;
    }

    public List<SproutResponse> Query(string query)
    {
        HttpResponseMessage response;
        using (var request = _client.CreateQueryRequest(query, Name))
        {
            try
            {
                response = _client.Http.Send(request);
            }
            catch (NotSupportedException)
            {
                // Handler without synchronous Send (some DelegatingHandlers, test servers):
                // block on the async path instead — prefer QueryAsync in async code
                return QueryAsync(query).AsTask().GetAwaiter().GetResult();
            }
        }

        using (response)
        {
            using var stream = response.Content.ReadAsStream();
            return ReadResponses(response.StatusCode, stream, query);
        }
    }

    /// <summary>
    /// The cancellation token only applies until the request is sent: once the server
    /// may be executing it, the call waits for the answer — so, as with the embedded
    /// engine, an <see cref="OperationCanceledException"/> means nothing was executed.
    /// </summary>
    public async ValueTask<List<SproutResponse>> QueryAsync(string query, CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();

        using var request = _client.CreateQueryRequest(query, Name);
        using var response = await _client.Http.SendAsync(request, CancellationToken.None).ConfigureAwait(false);
        await using var stream = await response.Content.ReadAsStreamAsync(CancellationToken.None).ConfigureAwait(false);
        return ReadResponses(response.StatusCode, stream, query);
    }

    /// <summary>
    /// Subscribes via the server's SignalR hub (<c>MapSproutDBHub()</c>). Each
    /// subscription has its own hub connection — the hub message carries no table
    /// name, so one connection per table keeps the callbacks apart.
    /// </summary>
    public IDisposable OnChange(string table, Action<SproutResponse> callback)
    {
        var connection = _client.CreateChangeConnection();
        connection.On<SproutResponse>("OnChange", callback);

        // Subscribe on every (re)connect — group membership does not survive a reconnect
        connection.Reconnected += _ => connection.InvokeAsync("Subscribe", Name, table);

        connection.StartAsync().GetAwaiter().GetResult();
        connection.InvokeAsync("Subscribe", Name, table).GetAwaiter().GetResult();

        return new ChangeSubscription(connection);
    }

    public void SaveQuery(string name, string query, bool pinned = false)
        => throw new NotSupportedException(
            "SaveQuery writes the protected _saved_queries table and is only available on the server (embedded engine).");

    private static List<SproutResponse> ReadResponses(HttpStatusCode status, Stream stream, string query)
    {
        // 200 / 401 / 403 carry the usual JSON array of responses
        if (status is HttpStatusCode.OK or HttpStatusCode.Unauthorized or HttpStatusCode.Forbidden)
        {
            var responses = JsonSerializer.Deserialize<List<SproutResponse>>(stream, SproutClientJson.Http);
            return responses ?? [];
        }

        // 400: {"error": "..."} — surface it like a query error, as the engine would
        if (status == HttpStatusCode.BadRequest)
        {
            using var doc = JsonDocument.Parse(stream);
            var message = doc.RootElement.ValueKind == JsonValueKind.Object
                && doc.RootElement.TryGetProperty("error", out var error)
                ? error.GetString() ?? "bad request"
                : "bad request";
            return [ResponseHelper.Error(query, ErrorCodes.SYNTAX_ERROR, message)];
        }

        throw new HttpRequestException($"SproutDB server returned HTTP {(int)status}", null, status);
    }

    private sealed class ChangeSubscription(HubConnection connection) : IDisposable
    {
        private int _disposed;

        public void Dispose()
        {
            if (Interlocked.Exchange(ref _disposed, 1) != 0)
                return;
            connection.DisposeAsync().AsTask().GetAwaiter().GetResult();
        }
    }
}

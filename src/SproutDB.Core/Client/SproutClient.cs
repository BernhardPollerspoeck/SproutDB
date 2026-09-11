using System.Net.Http.Headers;
using System.Reflection;
using System.Text;
using Microsoft.AspNetCore.SignalR.Client;
using Microsoft.Extensions.DependencyInjection;
using SproutDB.Core.Linq;

namespace SproutDB.Core.Client;

/// <summary>
/// <see cref="ISproutServer"/> for a SproutDB server reached over HTTP — the remote
/// counterpart of the embedded engine. Code written against <see cref="ISproutServer"/> /
/// <see cref="ISproutDatabase"/> (queries, parameters, typed API, QueryAsync, OnChange)
/// runs unchanged; embedded vs. remote is only a matter of registration
/// (<c>AddSproutDB</c> vs. <c>AddSproutDBClient</c>).
/// Not available remotely: <see cref="GetDatabases"/>, <see cref="Migrate"/> and
/// <c>SaveQuery</c> — they need server-side access.
/// </summary>
public sealed class SproutClient : ISproutServer, IDisposable
{
    private const string ApiKeyHeader = "X-SproutDB-ApiKey";
    private const string DatabaseHeader = "X-SproutDB-Database";

    private readonly bool _ownsHttpClient;
    private readonly Uri _baseAddress;
    private readonly string? _apiKey;
    private readonly Action<Microsoft.AspNetCore.Http.Connections.Client.HttpConnectionOptions>? _configureChangeConnection;

    internal HttpClient Http { get; }

    /// <summary>
    /// Creates a client with its own <see cref="HttpClient"/>.
    /// </summary>
    public SproutClient(SproutClientOptions options)
        : this(new HttpClient(new SocketsHttpHandler { PooledConnectionLifetime = TimeSpan.FromMinutes(2) })
        {
            Timeout = options.Timeout,
        }, options, ownsHttpClient: true)
    {
    }

    /// <summary>
    /// Creates a client on an existing <see cref="HttpClient"/>. Its <c>BaseAddress</c>
    /// is used unless <see cref="SproutClientOptions.BaseAddress"/> is set.
    /// </summary>
    public SproutClient(HttpClient httpClient, SproutClientOptions? options = null)
        : this(httpClient, options ?? new SproutClientOptions(), ownsHttpClient: false)
    {
    }

    private SproutClient(HttpClient httpClient, SproutClientOptions options, bool ownsHttpClient)
    {
        var baseAddress = options.BaseAddress ?? httpClient.BaseAddress
            ?? throw new ArgumentException("SproutClient needs a BaseAddress (options or HttpClient).", nameof(options));

        // Relative URLs must resolve below the base path
        _baseAddress = baseAddress.AbsoluteUri.EndsWith('/') ? baseAddress : new Uri(baseAddress.AbsoluteUri + "/");
        _apiKey = options.ApiKey;
        _configureChangeConnection = options.ConfigureChangeConnection;
        _ownsHttpClient = ownsHttpClient;
        Http = httpClient;
    }

    /// <summary>
    /// Returns the database, creating it on the server if it does not exist yet.
    /// </summary>
    public ISproutDatabase GetOrCreateDatabase(string name)
    {
        ValidateName(name);
        var db = new SproutRemoteDatabase(this, name.ToLowerInvariant());

        var result = db.Query("create database")[0];
        if (result.Errors is { Count: > 0 } errors && errors[0].Code != "DATABASE_EXISTS")
            throw new SproutQueryException(errors[0].Message);

        return db;
    }

    /// <summary>
    /// Returns the database. Throws if it does not exist on the server.
    /// </summary>
    public ISproutDatabase SelectDatabase(string name)
    {
        ValidateName(name);
        var db = new SproutRemoteDatabase(this, name.ToLowerInvariant());

        var result = db.Query("describe")[0];
        if (result.Errors is { Count: > 0 } errors)
        {
            if (errors[0].Code == "UNKNOWN_DATABASE")
                throw new InvalidOperationException($"database '{db.Name}' does not exist");
            throw new SproutQueryException(errors[0].Message);
        }

        return db;
    }

    /// <summary>Not available over HTTP.</summary>
    public IReadOnlyList<ISproutDatabase> GetDatabases()
        => throw new NotSupportedException("Listing databases is not available over HTTP.");

    /// <summary>Not available over HTTP — run migrations on the server (<c>AddMigrations</c>).</summary>
    public void Migrate(Assembly assembly, ISproutDatabase database)
        => throw new NotSupportedException(
            "Migrations write the protected _migrations table and run on the server — register them there with AddMigrations().");

    public void Dispose()
    {
        if (_ownsHttpClient)
            Http.Dispose();
    }

    internal HttpRequestMessage CreateQueryRequest(string query, string database)
    {
        var request = new HttpRequestMessage(HttpMethod.Post, new Uri(_baseAddress, "sproutdb/query"))
        {
            Content = new StringContent(query, Encoding.UTF8, new MediaTypeHeaderValue("text/plain")),
        };
        request.Headers.Add(DatabaseHeader, database);
        if (_apiKey is not null)
            request.Headers.Add(ApiKeyHeader, _apiKey);
        return request;
    }

    internal HubConnection CreateChangeConnection()
    {
        return new HubConnectionBuilder()
            .WithUrl(new Uri(_baseAddress, "sproutdb/changes"), options =>
            {
                if (_apiKey is not null)
                    options.Headers[ApiKeyHeader] = _apiKey;
                _configureChangeConnection?.Invoke(options);
            })
            .AddJsonProtocol(o => SproutClientJson.AddRowConverter(o.PayloadSerializerOptions))
            .WithAutomaticReconnect()
            .Build();
    }

    private static void ValidateName(string name)
    {
        if (!SproutEngine.IsValidName(name.ToLowerInvariant()))
            throw new InvalidDatabaseNameException(name, nameof(name));
    }
}

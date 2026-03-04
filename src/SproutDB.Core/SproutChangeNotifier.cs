using System.Collections.Concurrent;
using System.Threading.Channels;

namespace SproutDB.Core;

/// <summary>
/// Decoupled notification service for change events.
/// Writer thread enqueues via non-blocking <see cref="Enqueue"/>;
/// a separate dispatch loop delivers to in-process callbacks and (optionally) SignalR.
/// </summary>
public sealed class SproutChangeNotifier : IDisposable
{
    private readonly Channel<ChangeEvent> _channel;
    private readonly Task _dispatchTask;
    private readonly CancellationTokenSource _cts = new();

    // In-Process Callbacks: "{database}.{table}" → List<callback>
    private readonly ConcurrentDictionary<string, List<Action<SproutResponse>>> _callbacks = new();
    private readonly List<Action<string, string, SproutResponse>> _globalCallbacks = new();
    private readonly object _callbackLock = new();

    /// <summary>
    /// SignalR broadcast hook. Set by <c>MapSproutDBHub()</c> when SignalR is configured.
    /// Signature: (database, table, response) → fire-and-forget hub broadcast.
    /// </summary>
    internal Action<string, string, SproutResponse>? HubBroadcast { get; set; }

    public SproutChangeNotifier()
    {
        _channel = Channel.CreateUnbounded<ChangeEvent>(new UnboundedChannelOptions
        {
            SingleReader = true,
            SingleWriter = false,
        });

        _dispatchTask = Task.Factory.StartNew(
            () => RunDispatchLoop(_cts.Token),
            CancellationToken.None,
            TaskCreationOptions.LongRunning,
            TaskScheduler.Default).Unwrap();
    }

    /// <summary>
    /// Called by the writer thread after a successful mutation.
    /// Non-blocking — uses <see cref="Channel{T}.Writer.TryWrite"/>.
    /// </summary>
    internal void Enqueue(string database, string table, SproutResponse response)
    {
        _channel.Writer.TryWrite(new ChangeEvent(database, table, response));
    }

    /// <summary>
    /// Subscribe to ALL change events across all tables and databases.
    /// Returns an <see cref="IDisposable"/> to unsubscribe.
    /// </summary>
    public IDisposable SubscribeAll(Action<string, string, SproutResponse> callback)
    {
        lock (_callbackLock)
        {
            _globalCallbacks.Add(callback);
        }

        return new GlobalSubscription(this, callback);
    }

    /// <summary>
    /// Subscribe to change events for a specific table.
    /// Returns an <see cref="IDisposable"/> to unsubscribe.
    /// </summary>
    public IDisposable Subscribe(string database, string table, Action<SproutResponse> callback)
    {
        var key = $"{database}.{table}";

        lock (_callbackLock)
        {
            if (!_callbacks.TryGetValue(key, out var list))
            {
                list = new List<Action<SproutResponse>>();
                _callbacks[key] = list;
            }
            list.Add(callback);
        }

        return new Subscription(this, key, callback);
    }

    private void UnsubscribeGlobal(Action<string, string, SproutResponse> callback)
    {
        lock (_callbackLock)
        {
            _globalCallbacks.Remove(callback);
        }
    }

    private void Unsubscribe(string key, Action<SproutResponse> callback)
    {
        lock (_callbackLock)
        {
            if (_callbacks.TryGetValue(key, out var list))
            {
                list.Remove(callback);
                if (list.Count == 0)
                    _callbacks.TryRemove(key, out _);
            }
        }
    }

    private async Task RunDispatchLoop(CancellationToken ct)
    {
        try
        {
            await foreach (var evt in _channel.Reader.ReadAllAsync(ct))
            {
                Dispatch(evt);
            }
        }
        catch (OperationCanceledException)
        {
            // Shutdown — drain remaining events
        }

        // Drain anything left in the channel after cancellation
        while (_channel.Reader.TryRead(out var remaining))
        {
            Dispatch(remaining);
        }
    }

    private void Dispatch(ChangeEvent evt)
    {
        var key = $"{evt.Database}.{evt.Table}";

        // In-process per-key callbacks
        List<Action<SproutResponse>>? snapshot = null;
        List<Action<string, string, SproutResponse>>? globalSnapshot = null;
        lock (_callbackLock)
        {
            if (_callbacks.TryGetValue(key, out var list))
                snapshot = new List<Action<SproutResponse>>(list);
            if (_globalCallbacks.Count > 0)
                globalSnapshot = new List<Action<string, string, SproutResponse>>(_globalCallbacks);
        }

        if (snapshot is not null)
        {
            foreach (var callback in snapshot)
            {
                try
                {
                    callback(evt.Response);
                }
                catch
                {
                    // Swallow — callback exceptions must never block dispatch
                }
            }
        }

        // Global callbacks
        if (globalSnapshot is not null)
        {
            foreach (var callback in globalSnapshot)
            {
                try
                {
                    callback(evt.Database, evt.Table, evt.Response);
                }
                catch
                {
                    // Swallow — callback exceptions must never block dispatch
                }
            }
        }

        // SignalR broadcast (if configured)
        try
        {
            HubBroadcast?.Invoke(evt.Database, evt.Table, evt.Response);
        }
        catch
        {
            // Swallow — hub errors must never block dispatch
        }
    }

    public void Dispose()
    {
        _cts.Cancel();
        _channel.Writer.Complete();
        _dispatchTask.GetAwaiter().GetResult();
        _cts.Dispose();
    }

    private sealed class Subscription : IDisposable
    {
        private readonly SproutChangeNotifier _notifier;
        private readonly string _key;
        private readonly Action<SproutResponse> _callback;

        public Subscription(SproutChangeNotifier notifier, string key, Action<SproutResponse> callback)
        {
            _notifier = notifier;
            _key = key;
            _callback = callback;
        }

        public void Dispose() => _notifier.Unsubscribe(_key, _callback);
    }

    private sealed class GlobalSubscription : IDisposable
    {
        private readonly SproutChangeNotifier _notifier;
        private readonly Action<string, string, SproutResponse> _callback;

        public GlobalSubscription(SproutChangeNotifier notifier, Action<string, string, SproutResponse> callback)
        {
            _notifier = notifier;
            _callback = callback;
        }

        public void Dispose() => _notifier.UnsubscribeGlobal(_callback);
    }
}

internal readonly record struct ChangeEvent(string Database, string Table, SproutResponse Response);

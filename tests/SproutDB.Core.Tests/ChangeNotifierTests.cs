namespace SproutDB.Core.Tests;

public class ChangeNotifierTests : IDisposable
{
    private readonly SproutChangeNotifier _notifier = new();

    public void Dispose() => _notifier.Dispose();

    [Fact]
    public void Enqueue_CallsSubscribedCallback()
    {
        using var signal = new ManualResetEventSlim();
        var received = new List<SproutResponse>();
        _notifier.Subscribe("shop", "users", r => { received.Add(r); signal.Set(); });

        var response = MakeResponse(SproutOperation.Upsert, 1);
        _notifier.Enqueue("shop", "users", response);

        Assert.True(signal.Wait(3000));
        Assert.Single(received);
        Assert.Same(response, received[0]);
    }

    [Fact]
    public void Enqueue_DoesNotCallUnrelatedSubscription()
    {
        var received = new List<SproutResponse>();
        _notifier.Subscribe("shop", "orders", r => received.Add(r));

        // Subscribe to the target to know when dispatch is done
        using var signal = new ManualResetEventSlim();
        _notifier.Subscribe("shop", "users", _ => signal.Set());

        _notifier.Enqueue("shop", "users", MakeResponse(SproutOperation.Upsert, 1));

        Assert.True(signal.Wait(3000));
        Assert.Empty(received);
    }

    [Fact]
    public void Unsubscribe_StopsCallbacks()
    {
        using var signal1 = new ManualResetEventSlim();
        using var signal2 = new ManualResetEventSlim();

        var received = new List<SproutResponse>();
        var sub = _notifier.Subscribe("shop", "users", r =>
        {
            received.Add(r);
            if (received.Count == 1) signal1.Set();
        });

        // Use a sentinel callback that stays active
        _notifier.Subscribe("shop", "users", _ => signal2.Set());

        _notifier.Enqueue("shop", "users", MakeResponse(SproutOperation.Upsert, 1));
        Assert.True(signal1.Wait(3000));

        sub.Dispose(); // unsubscribe

        signal2.Reset();
        _notifier.Enqueue("shop", "users", MakeResponse(SproutOperation.Upsert, 1));
        Assert.True(signal2.Wait(3000));

        Assert.Single(received); // only the first one
    }

    [Fact]
    public void MultipleSubscribers_AllReceive()
    {
        using var signal = new CountdownEvent(2);
        var received1 = new List<SproutResponse>();
        var received2 = new List<SproutResponse>();
        _notifier.Subscribe("shop", "users", r => { received1.Add(r); signal.Signal(); });
        _notifier.Subscribe("shop", "users", r => { received2.Add(r); signal.Signal(); });

        _notifier.Enqueue("shop", "users", MakeResponse(SproutOperation.Upsert, 1));

        Assert.True(signal.Wait(3000));
        Assert.Single(received1);
        Assert.Single(received2);
    }

    [Fact]
    public void CallbackException_DoesNotBlockOtherCallbacks()
    {
        using var signal = new ManualResetEventSlim();
        var received = new List<SproutResponse>();
        _notifier.Subscribe("shop", "users", _ => throw new InvalidOperationException("boom"));
        _notifier.Subscribe("shop", "users", r => { received.Add(r); signal.Set(); });

        _notifier.Enqueue("shop", "users", MakeResponse(SproutOperation.Upsert, 1));

        Assert.True(signal.Wait(3000));
        Assert.Single(received);
    }

    [Fact]
    public void HubBroadcast_CalledWhenSet()
    {
        using var signal = new ManualResetEventSlim();
        var broadcasts = new List<(string Db, string Table, SproutResponse Response)>();
        _notifier.HubBroadcast = (db, table, r) => { broadcasts.Add((db, table, r)); signal.Set(); };

        var response = MakeResponse(SproutOperation.Upsert, 1);
        _notifier.Enqueue("shop", "users", response);

        Assert.True(signal.Wait(3000));
        Assert.Single(broadcasts);
        Assert.Equal("shop", broadcasts[0].Db);
        Assert.Equal("users", broadcasts[0].Table);
        Assert.Same(response, broadcasts[0].Response);
    }

    [Fact]
    public void HubBroadcastException_DoesNotBlockCallbacks()
    {
        using var signal = new ManualResetEventSlim();
        _notifier.HubBroadcast = (_, _, _) => throw new InvalidOperationException("hub down");

        var received = new List<SproutResponse>();
        _notifier.Subscribe("shop", "users", r => { received.Add(r); signal.Set(); });

        _notifier.Enqueue("shop", "users", MakeResponse(SproutOperation.Upsert, 1));

        Assert.True(signal.Wait(3000));
        Assert.Single(received);
    }

    [Fact]
    public void SchemaEvent_RoutedToSchemaKey()
    {
        using var signal = new ManualResetEventSlim();
        var received = new List<SproutResponse>();
        _notifier.Subscribe("shop", "_schema", r => { received.Add(r); signal.Set(); });

        _notifier.Enqueue("shop", "_schema", MakeResponse(SproutOperation.CreateTable, 0));

        Assert.True(signal.Wait(3000));
        Assert.Single(received);
    }

    private static SproutResponse MakeResponse(SproutOperation op, int affected)
        => new() { Operation = op, Affected = affected };
}

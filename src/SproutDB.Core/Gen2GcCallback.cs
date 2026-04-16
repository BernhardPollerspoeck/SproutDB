namespace SproutDB.Core;

/// <summary>
/// Invokes a callback after each Gen2 GC, without holding a strong reference
/// that would itself prevent collection. Uses the finalizer + re-registration
/// pattern recommended by the .NET team (see dotnet/runtime source for the
/// same technique in MemoryCache).
///
/// The target object is held via a WeakReference; when it is collected, the
/// callback self-terminates.
/// </summary>
internal sealed class Gen2GcCallback
{
    private readonly Func<object, bool> _callback;
    private readonly WeakReference _target;

    private Gen2GcCallback(Func<object, bool> callback, object targetObj)
    {
        _callback = callback;
        _target = new WeakReference(targetObj);
    }

    /// <summary>
    /// Registers <paramref name="callback"/> to be invoked after every Gen2 GC.
    /// The callback receives <paramref name="targetObj"/> as its argument and
    /// returns true to stay registered, false to stop.
    /// </summary>
    public static void Register(Func<object, bool> callback, object targetObj)
    {
        // Allocated object is kept alive by the GC's finalizer queue.
        _ = new Gen2GcCallback(callback, targetObj);
    }

    ~Gen2GcCallback()
    {
        var target = _target.Target;
        if (target is null)
            return; // target was collected, let this instance go too

        try
        {
            if (!_callback(target))
                return; // callback asked to unregister
        }
        catch
        {
            // Swallow — we must never throw from a finalizer
            return;
        }

        // Re-register for the next Gen2 GC
        if (!Environment.HasShutdownStarted && !AppDomain.CurrentDomain.IsFinalizingForUnload())
            GC.ReRegisterForFinalize(this);
    }
}

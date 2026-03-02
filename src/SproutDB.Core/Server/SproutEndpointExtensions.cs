using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Routing;
using Microsoft.AspNetCore.SignalR;
using Microsoft.Extensions.DependencyInjection;

namespace SproutDB.Core.Server;

public static class SproutEndpointExtensions
{
    public static IEndpointRouteBuilder MapSproutDB(this IEndpointRouteBuilder endpoints)
    {
        SproutEndpoints.Map(endpoints);
        return endpoints;
    }

    /// <summary>
    /// Maps the SignalR hub for real-time change notifications at <c>/sproutdb/changes</c>.
    /// Wires up the <see cref="SproutChangeNotifier.HubBroadcast"/> hook so that
    /// mutations are broadcast to subscribed clients.
    /// </summary>
    public static IEndpointRouteBuilder MapSproutDBHub(this IEndpointRouteBuilder endpoints)
    {
        endpoints.MapHub<SproutChangeHub>("/sproutdb/changes");

        var engine = endpoints.ServiceProvider.GetRequiredService<SproutEngine>();
        var hubContext = endpoints.ServiceProvider.GetRequiredService<IHubContext<SproutChangeHub>>();

        engine.ChangeNotifier.HubBroadcast = (db, table, response) =>
        {
            var group = $"{db}.{table}";
            // Fire-and-forget — dispatch loop handles errors
            _ = hubContext.Clients.Group(group).SendAsync("OnChange", response);
        };

        return endpoints;
    }
}

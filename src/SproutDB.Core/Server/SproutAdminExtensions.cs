using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http.HttpResults;
using Microsoft.AspNetCore.Routing;
using Microsoft.Extensions.DependencyInjection;
using SproutDB.Core.Admin.Components;

namespace SproutDB.Core.Server;

/// <summary>
/// Extension methods for registering the SproutDB Admin UI.
/// </summary>
public static class SproutAdminExtensions
{
    /// <summary>
    /// Maps the SproutDB Admin UI at <c>/sproutdb/admin</c>.
    /// Requires <see cref="AddSproutDBAdmin"/> to be called on the service collection.
    /// </summary>
    public static IEndpointRouteBuilder MapSproutDBAdmin(this IEndpointRouteBuilder endpoints)
    {
        endpoints.MapRazorComponents<AdminApp>()
            .AddInteractiveServerRenderMode();

        return endpoints;
    }

    /// <summary>
    /// Registers services required by the SproutDB Admin UI (Blazor SSR).
    /// Call this before <see cref="MapSproutDBAdmin"/>.
    /// </summary>
    public static IServiceCollection AddSproutDBAdmin(this IServiceCollection services)
    {
        services.AddRazorComponents()
            .AddInteractiveServerComponents();
        return services;
    }
}

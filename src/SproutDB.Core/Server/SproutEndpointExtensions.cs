using Microsoft.AspNetCore.Routing;

namespace SproutDB.Core.Server;

public static class SproutEndpointExtensions
{
    public static IEndpointRouteBuilder MapSproutDB(this IEndpointRouteBuilder endpoints)
    {
        SproutEndpoints.Map(endpoints);
        return endpoints;
    }
}

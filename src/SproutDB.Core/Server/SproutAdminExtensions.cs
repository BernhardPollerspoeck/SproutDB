using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
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

        endpoints.MapGet("/sproutdb/admin/export/{db}", HandleExport);
        endpoints.MapPost("/sproutdb/admin/import/{db}", HandleImport)
            .DisableAntiforgery();

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

    private static async Task<IResult> HandleExport(string db, HttpContext context, SproutEngine engine)
    {
        // Auth check: if auth is enabled, require MasterKey or admin role
        if (engine.AuthService is not null)
        {
            var apiKeyHeader = context.Request.Headers["X-SproutDB-ApiKey"].ToString();

            if (string.IsNullOrWhiteSpace(apiKeyHeader))
            {
                // Also check query string for browser-initiated downloads
                apiKeyHeader = context.Request.Query["key"].ToString();
            }

            if (string.IsNullOrWhiteSpace(apiKeyHeader))
                return Results.Json(new { error = "missing required header: X-SproutDB-ApiKey" }, statusCode: 401);

            if (!engine.AuthService.IsMasterKey(apiKeyHeader))
            {
                var key = engine.AuthService.ValidateKey(apiKeyHeader);
                if (key is null)
                    return Results.Json(new { error = "invalid api key" }, statusCode: 401);

                var hasAdmin = key.Permissions.TryGetValue(db.ToLowerInvariant(), out var role)
                    && string.Equals(role, "admin", StringComparison.OrdinalIgnoreCase);
                if (!hasAdmin)
                    return Results.Json(new { error = "requires master key or admin on database" }, statusCode: 403);
            }
        }

        var response = engine.Execute("backup", db);

        if (response.Operation == SproutOperation.Error)
        {
            var msg = response.Errors?.Count > 0 ? response.Errors[0].Message : "backup failed";
            return Results.BadRequest(new { error = msg });
        }

        var zipPath = response.BackupPath;
        if (zipPath is null || !File.Exists(zipPath))
            return Results.StatusCode(500);

        var fileName = Path.GetFileName(zipPath);

        // Stream the file and delete it afterwards
        var fileBytes = await File.ReadAllBytesAsync(zipPath);
        try { File.Delete(zipPath); } catch { /* best effort cleanup */ }

        return Results.File(fileBytes, "application/zip", fileName);
    }

    private static async Task<IResult> HandleImport(string db, HttpContext context, SproutEngine engine)
    {
        if (!context.Request.HasFormContentType)
            return Results.BadRequest(new { error = "expected multipart/form-data" });

        var form = await context.Request.ReadFormAsync();
        var file = form.Files.GetFile("file");

        if (file is null || file.Length == 0)
            return Results.BadRequest(new { error = "no file uploaded" });

        var tempPath = Path.Combine(Path.GetTempPath(), $"sproutdb_import_{Guid.NewGuid():N}.zip");
        try
        {
            await using (var fs = new FileStream(tempPath, FileMode.Create))
            {
                await file.CopyToAsync(fs);
            }

            var response = engine.Execute($"restore '{tempPath}'", db);

            if (response.Operation == SproutOperation.Error)
            {
                var msg = response.Errors?.Count > 0 ? response.Errors[0].Message : "restore failed";
                return Results.BadRequest(new { error = msg });
            }

            return Results.Ok(new { message = $"Database '{db}' restored successfully" });
        }
        finally
        {
            try { File.Delete(tempPath); } catch { /* best effort */ }
        }
    }
}

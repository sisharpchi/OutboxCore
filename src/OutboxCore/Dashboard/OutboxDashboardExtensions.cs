using Microsoft.AspNetCore.Builder;

namespace OutboxCore.Dashboard;

public static class OutboxDashboardExtensions
{
    public static IApplicationBuilder UseOutboxDashboard(
        this IApplicationBuilder app,
        string pathPrefix = "/outbox-dashboard")
    {
        return app.UseMiddleware<OutboxDashboardMiddleware>(pathPrefix);
    }
}

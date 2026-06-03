using System;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using OutboxCore.Background;

namespace OutboxCore.Configuration;

public static class OutboxServiceCollectionExtensions
{
    public static OutboxBuilder AddOutboxCore(this IServiceCollection services, Action<OutboxOptions>? configureOptions = null)
    {
        if (configureOptions != null)
        {
            services.Configure(configureOptions);
        }
        else
        {
            services.AddOptions<OutboxOptions>();
        }

        services.TryAddSingleton<IOutboxChannel, OutboxChannel>();
        services.AddHostedService<OutboxPublisherBackgroundService>();

        return new OutboxBuilder(services);
    }
}

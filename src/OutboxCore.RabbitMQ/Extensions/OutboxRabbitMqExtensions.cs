using System;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Options;
using OutboxCore.Abstractions;
using OutboxCore.Configuration;
using OutboxCore.RabbitMQ.Configuration;

namespace OutboxCore.RabbitMQ.Extensions;

public static class OutboxRabbitMqExtensions
{
    public static OutboxBuilder UseRabbitMq(
        this OutboxBuilder builder,
        Action<RabbitMqOptions>? configureOptions = null)
    {
        if (configureOptions != null)
        {
            builder.Services.Configure(configureOptions);
        }
        else
        {
            builder.Services.AddOptions<RabbitMqOptions>();
        }

        builder.Services.TryAddSingleton<IMessagePublisher, RabbitMqMessagePublisher>();

        return builder;
    }

    public static OutboxModuleBuilder UseRabbitMq(
        this OutboxModuleBuilder builder,
        Action<RabbitMqOptions>? configureOptions = null)
    {
        if (configureOptions != null)
        {
            builder.Services.Configure<RabbitMqOptions>(builder.ModuleName, configureOptions);
        }

        builder.Services.TryAddKeyedSingleton<IMessagePublisher>(builder.ModuleName, (sp, key) =>
        {
            var optionsMonitor = sp.GetRequiredService<IOptionsMonitor<RabbitMqOptions>>();
            var options = optionsMonitor.Get((string)key!);
            return new RabbitMqMessagePublisher(options);
        });

        return builder;
    }
}

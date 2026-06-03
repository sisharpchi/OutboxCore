using System;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
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
}

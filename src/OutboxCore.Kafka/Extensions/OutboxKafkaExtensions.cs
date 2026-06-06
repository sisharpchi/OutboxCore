using System;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Options;
using OutboxCore.Abstractions;
using OutboxCore.Configuration;
using OutboxCore.Kafka.Configuration;

namespace OutboxCore.Kafka.Extensions;

public static class OutboxKafkaExtensions
{
    public static OutboxBuilder UseKafka(
        this OutboxBuilder builder,
        Action<KafkaOptions>? configureOptions = null)
    {
        if (configureOptions != null)
        {
            builder.Services.Configure(configureOptions);
        }
        else
        {
            builder.Services.AddOptions<KafkaOptions>();
        }

        builder.Services.TryAdd(ServiceDescriptor.Singleton<IMessagePublisher>(sp =>
        {
            var options = sp.GetRequiredService<IOptions<KafkaOptions>>().Value;
            return new KafkaMessagePublisher(options);
        }));

        return builder;
    }

    public static OutboxModuleBuilder UseKafka(
        this OutboxModuleBuilder builder,
        Action<KafkaOptions>? configureOptions = null)
    {
        if (configureOptions != null)
        {
            builder.Services.Configure<KafkaOptions>(builder.ModuleName, configureOptions);
        }

        builder.Services.TryAddKeyedSingleton<IMessagePublisher>(builder.ModuleName, (sp, key) =>
        {
            var optionsMonitor = sp.GetRequiredService<IOptionsMonitor<KafkaOptions>>();
            var options = optionsMonitor.Get((string)key!);
            return new KafkaMessagePublisher(options);
        });

        return builder;
    }
}

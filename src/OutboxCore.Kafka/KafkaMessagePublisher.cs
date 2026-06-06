using System.Threading;
using System.Threading.Tasks;
using OutboxCore.Abstractions;
using OutboxCore.Kafka.Configuration;

namespace OutboxCore.Kafka;

public class KafkaMessagePublisher : IMessagePublisher
{
    private readonly KafkaOptions _options;

    public KafkaMessagePublisher(KafkaOptions options)
    {
        _options = options;
    }

    public Task PublishAsync<T>(T message, CancellationToken cancellationToken = default) where T : class
    {
        return Task.CompletedTask;
    }

    public Task PublishAsync(string messageType, string content, CancellationToken cancellationToken = default)
    {
        return Task.CompletedTask;
    }
}

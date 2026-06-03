using System.Threading;
using System.Threading.Tasks;

namespace OutboxCore.Abstractions;

public interface IMessagePublisher
{
    Task PublishAsync<T>(T message, CancellationToken cancellationToken = default) where T : class;
    Task PublishAsync(string messageType, string content, CancellationToken cancellationToken = default);
}

using System.Collections.Concurrent;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using OutboxCore.Abstractions;

namespace OutboxCore.Tests.E2e.Fixtures;

public class InMemoryMessagePublisher : IMessagePublisher
{
    public ConcurrentQueue<(string MessageType, string Content)> PublishedMessages { get; } = new();

    public Task PublishAsync<T>(T message, CancellationToken cancellationToken = default) where T : class
    {
        string messageType = typeof(T).FullName ?? typeof(T).Name;
        string content = JsonSerializer.Serialize(message);
        PublishedMessages.Enqueue((messageType, content));
        return Task.CompletedTask;
    }

    public Task PublishAsync(string messageType, string content, CancellationToken cancellationToken = default)
    {
        PublishedMessages.Enqueue((messageType, content));
        return Task.CompletedTask;
    }

    public void Clear()
    {
        PublishedMessages.Clear();
    }
}

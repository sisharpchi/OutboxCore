using System;
using System.Threading;
using System.Threading.Tasks;

namespace OutboxCore.Abstractions;

public interface IInboxProcessor
{
    Task<bool> HasBeenProcessedAsync(Guid messageId, CancellationToken cancellationToken = default);
    Task TrackMessageAsync(Guid messageId, string messageType, CancellationToken cancellationToken = default);
    Task MarkAsProcessedAsync(Guid messageId, CancellationToken cancellationToken = default);
    Task MarkAsFailedAsync(Guid messageId, string error, CancellationToken cancellationToken = default);
}

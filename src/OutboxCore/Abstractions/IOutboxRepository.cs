using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using OutboxCore.Models;

namespace OutboxCore.Abstractions;

public interface IOutboxRepository
{
    Task<IReadOnlyList<OutboxMessage>> LockMessagesAsync(string workerId, TimeSpan lockDuration, int batchSize, CancellationToken cancellationToken);
    Task UpdateMessageStatusAsync(Guid messageId, string status, DateTimeOffset? processedAt, string? error, int retryCount, CancellationToken cancellationToken);
    Task DeleteMessageAsync(Guid messageId, CancellationToken cancellationToken);
}

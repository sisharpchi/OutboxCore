using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using OutboxCore.Models;

namespace OutboxCore.Abstractions;

public interface IOutboxRepository
{
    Task<IReadOnlyList<OutboxMessage>> LockMessagesAsync(string moduleName, string workerId, TimeSpan lockDuration, int batchSize, CancellationToken cancellationToken);
    Task UpdateMessageStatusAsync(string moduleName, Guid messageId, string status, DateTimeOffset? processedAt, string? error, int retryCount, CancellationToken cancellationToken);
    Task DeleteMessageAsync(string moduleName, Guid messageId, CancellationToken cancellationToken);
    Task<IReadOnlyList<OutboxMessage>> GetMessagesAsync(string moduleName, string? status, int limit, CancellationToken cancellationToken);
    Task<int> GetCountByStatusAsync(string moduleName, string status, CancellationToken cancellationToken);
    Task DeleteOldMessagesAsync(string moduleName, DateTimeOffset olderThan, CancellationToken cancellationToken);
}

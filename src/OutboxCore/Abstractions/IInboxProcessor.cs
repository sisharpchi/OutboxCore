using System;
using System.Threading;
using System.Threading.Tasks;
using OutboxCore.Models;

namespace OutboxCore.Abstractions;

public interface IInboxProcessor
{
    Task<bool> HasBeenProcessedAsync(string moduleName, Guid messageId, CancellationToken cancellationToken = default);
    Task TrackMessageAsync(string moduleName, Guid messageId, string messageType, CancellationToken cancellationToken = default);
    Task MarkAsProcessedAsync(string moduleName, Guid messageId, CancellationToken cancellationToken = default);
    Task MarkAsFailedAsync(string moduleName, Guid messageId, string error, CancellationToken cancellationToken = default);
    Task<IReadOnlyList<InboxMessage>> GetMessagesAsync(string moduleName, string? status, int limit, CancellationToken cancellationToken = default);
    Task<int> GetCountByStatusAsync(string moduleName, string status, CancellationToken cancellationToken = default);
    Task DeleteOldMessagesAsync(string moduleName, DateTimeOffset olderThan, CancellationToken cancellationToken = default);
}

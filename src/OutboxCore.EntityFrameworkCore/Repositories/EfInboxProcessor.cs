using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.EntityFrameworkCore;
using OutboxCore.Abstractions;
using OutboxCore.Models;

namespace OutboxCore.EntityFrameworkCore.Repositories;

public class EfInboxProcessor<TContext> : IInboxProcessor where TContext : DbContext
{
    private readonly TContext _dbContext;

    public EfInboxProcessor(TContext dbContext)
    {
        _dbContext = dbContext;
    }

    public async Task<bool> HasBeenProcessedAsync(Guid messageId, CancellationToken cancellationToken = default)
    {
        var message = await _dbContext.Set<InboxMessage>().FindAsync(new object[] { messageId }, cancellationToken);
        return message != null && message.Status == "Processed";
    }

    public async Task TrackMessageAsync(Guid messageId, string messageType, CancellationToken cancellationToken = default)
    {
        var message = new InboxMessage
        {
            Id = messageId,
            MessageType = messageType,
            ReceivedAt = DateTimeOffset.UtcNow,
            Status = "Pending"
        };

        _dbContext.Set<InboxMessage>().Add(message);
        await _dbContext.SaveChangesAsync(cancellationToken);
    }

    public async Task MarkAsProcessedAsync(Guid messageId, CancellationToken cancellationToken = default)
    {
        var message = await _dbContext.Set<InboxMessage>().FindAsync(new object[] { messageId }, cancellationToken);
        if (message != null)
        {
            message.Status = "Processed";
            message.ProcessedAt = DateTimeOffset.UtcNow;
            _dbContext.Entry(message).State = EntityState.Modified;
            await _dbContext.SaveChangesAsync(cancellationToken);
        }
    }

    public async Task MarkAsFailedAsync(Guid messageId, string error, CancellationToken cancellationToken = default)
    {
        var message = await _dbContext.Set<InboxMessage>().FindAsync(new object[] { messageId }, cancellationToken);
        if (message != null)
        {
            message.Status = "Failed";
            message.Error = error;
            _dbContext.Entry(message).State = EntityState.Modified;
            await _dbContext.SaveChangesAsync(cancellationToken);
        }
    }
}

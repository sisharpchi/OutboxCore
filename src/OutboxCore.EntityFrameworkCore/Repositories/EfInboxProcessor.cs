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

    public async Task<bool> HasBeenProcessedAsync(string moduleName, Guid messageId, CancellationToken cancellationToken = default)
    {
        var message = await _dbContext.Set<InboxMessage>()
            .FirstOrDefaultAsync(x => x.Id == messageId && x.ModuleName == moduleName, cancellationToken);
        return message != null && message.Status == "Processed";
    }

    public async Task TrackMessageAsync(string moduleName, Guid messageId, string messageType, CancellationToken cancellationToken = default)
    {
        var message = new InboxMessage
        {
            Id = messageId,
            ModuleName = moduleName,
            MessageType = messageType,
            ReceivedAt = DateTimeOffset.UtcNow,
            Status = "Pending"
        };

        _dbContext.Set<InboxMessage>().Add(message);
        await _dbContext.SaveChangesAsync(cancellationToken);
    }

    public async Task MarkAsProcessedAsync(string moduleName, Guid messageId, CancellationToken cancellationToken = default)
    {
        var message = await _dbContext.Set<InboxMessage>()
            .FirstOrDefaultAsync(x => x.Id == messageId && x.ModuleName == moduleName, cancellationToken);
        if (message != null)
        {
            message.Status = "Processed";
            message.ProcessedAt = DateTimeOffset.UtcNow;
            _dbContext.Entry(message).State = EntityState.Modified;
            await _dbContext.SaveChangesAsync(cancellationToken);
        }
    }

    public async Task MarkAsFailedAsync(string moduleName, Guid messageId, string error, CancellationToken cancellationToken = default)
    {
        var message = await _dbContext.Set<InboxMessage>()
            .FirstOrDefaultAsync(x => x.Id == messageId && x.ModuleName == moduleName, cancellationToken);
        if (message != null)
        {
            message.Status = "Failed";
            message.Error = error;
            _dbContext.Entry(message).State = EntityState.Modified;
            await _dbContext.SaveChangesAsync(cancellationToken);
        }
    }

    public async Task<IReadOnlyList<InboxMessage>> GetMessagesAsync(string moduleName, string? status, int limit, CancellationToken cancellationToken = default)
    {
        IQueryable<InboxMessage> query = _dbContext.Set<InboxMessage>().Where(x => x.ModuleName == moduleName);
        if (!string.IsNullOrEmpty(status))
        {
            query = query.Where(x => x.Status == status);
        }

        if (_dbContext.Database.ProviderName == "Microsoft.EntityFrameworkCore.Sqlite")
        {
            var rawList = await query.ToListAsync(cancellationToken);
            return rawList.OrderByDescending(x => x.ReceivedAt).Take(limit).ToList();
        }
        else
        {
            return await query.OrderByDescending(x => x.ReceivedAt).Take(limit).ToListAsync(cancellationToken);
        }
    }

    public async Task<int> GetCountByStatusAsync(string moduleName, string status, CancellationToken cancellationToken = default)
    {
        return await _dbContext.Set<InboxMessage>().CountAsync(x => x.ModuleName == moduleName && x.Status == status, cancellationToken);
    }

    public async Task DeleteOldMessagesAsync(string moduleName, DateTimeOffset olderThan, CancellationToken cancellationToken = default)
    {
        if (_dbContext.Database.ProviderName == "Microsoft.EntityFrameworkCore.Sqlite")
        {
            var messages = await _dbContext.Set<InboxMessage>()
                .Where(x => x.ModuleName == moduleName && (x.Status == "Processed" || x.Status == "Failed"))
                .ToListAsync(cancellationToken);

            var toDelete = messages.Where(x => x.ReceivedAt < olderThan).ToList();

            if (toDelete.Count > 0)
            {
                _dbContext.Set<InboxMessage>().RemoveRange(toDelete);
                await _dbContext.SaveChangesAsync(cancellationToken);
            }
        }
        else
        {
            await _dbContext.Set<InboxMessage>()
                .Where(x => x.ModuleName == moduleName && (x.Status == "Processed" || x.Status == "Failed") && x.ReceivedAt < olderThan)
                .ExecuteDeleteAsync(cancellationToken);
        }
    }
}

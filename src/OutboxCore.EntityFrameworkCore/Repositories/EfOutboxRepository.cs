using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.EntityFrameworkCore;
using OutboxCore.Abstractions;
using OutboxCore.Models;

namespace OutboxCore.EntityFrameworkCore.Repositories;

public class EfOutboxRepository<TContext> : IOutboxRepository where TContext : DbContext
{
    private readonly TContext _dbContext;
    private readonly ISqlDialect _dialect;

    public EfOutboxRepository(TContext dbContext, ISqlDialect dialect)
    {
        _dbContext = dbContext;
        _dialect = dialect;
    }

    public async Task<IReadOnlyList<OutboxMessage>> LockMessagesAsync(
        string moduleName,
        string workerId,
        TimeSpan lockDuration,
        int batchSize,
        CancellationToken cancellationToken)
    {
        var entityType = _dbContext.Model.FindEntityType(typeof(OutboxMessage));
        var tableName = entityType?.GetTableName() ?? "OutboxMessages";
        var schema = entityType?.GetSchema();
        var sql = _dialect.GetLockMessagesSql(schema, tableName, batchSize);

        var now = DateTimeOffset.UtcNow;
        var lockedUntil = now.Add(lockDuration);

        // Get database-specific parameter types using DbConnection factory
        var connection = _dbContext.Database.GetDbConnection();
        var wasClosed = connection.State == System.Data.ConnectionState.Closed;
        if (wasClosed)
        {
            await _dbContext.Database.OpenConnectionAsync(cancellationToken);
        }

        try
        {
            using var command = connection.CreateCommand();

            var lockedUntilParam = command.CreateParameter();
            lockedUntilParam.ParameterName = "@LockedUntil";
            lockedUntilParam.Value = lockedUntil;

            var workerIdParam = command.CreateParameter();
            workerIdParam.ParameterName = "@WorkerId";
            workerIdParam.Value = workerId;

            var nowParam = command.CreateParameter();
            nowParam.ParameterName = "@Now";
            nowParam.Value = now;

            var moduleNameParam = command.CreateParameter();
            moduleNameParam.ParameterName = "@ModuleName";
            moduleNameParam.Value = moduleName;

            return await _dbContext.Set<OutboxMessage>()
                .FromSqlRaw(sql, lockedUntilParam, workerIdParam, nowParam, moduleNameParam)
                .ToListAsync(cancellationToken);
        }
        finally
        {
            if (wasClosed)
            {
                await _dbContext.Database.CloseConnectionAsync();
            }
        }
    }

    public async Task UpdateMessageStatusAsync(
        string moduleName,
        Guid messageId,
        string status,
        DateTimeOffset? processedAt,
        string? error,
        int retryCount,
        CancellationToken cancellationToken)
    {
        var message = await _dbContext.Set<OutboxMessage>()
            .FirstOrDefaultAsync(x => x.Id == messageId && x.ModuleName == moduleName, cancellationToken);
        if (message != null)
        {
            message.Status = status;
            message.ProcessedAt = processedAt;
            message.Error = error;
            message.RetryCount = retryCount;
            message.LockedUntil = null;
            message.WorkerId = null;

            _dbContext.Entry(message).State = EntityState.Modified;
            await _dbContext.SaveChangesAsync(cancellationToken);
        }
    }

    public async Task DeleteMessageAsync(string moduleName, Guid messageId, CancellationToken cancellationToken)
    {
        var message = await _dbContext.Set<OutboxMessage>()
            .FirstOrDefaultAsync(x => x.Id == messageId && x.ModuleName == moduleName, cancellationToken);
        if (message != null)
        {
            _dbContext.Set<OutboxMessage>().Remove(message);
            await _dbContext.SaveChangesAsync(cancellationToken);
        }
    }

    public async Task<IReadOnlyList<OutboxMessage>> GetMessagesAsync(string moduleName, string? status, int limit, CancellationToken cancellationToken)
    {
        IQueryable<OutboxMessage> query = _dbContext.Set<OutboxMessage>().Where(x => x.ModuleName == moduleName);
        if (!string.IsNullOrEmpty(status))
        {
            query = query.Where(x => x.Status == status);
        }

        if (_dbContext.Database.ProviderName == "Microsoft.EntityFrameworkCore.Sqlite")
        {
            var rawList = await query.ToListAsync(cancellationToken);
            return rawList.OrderByDescending(x => x.CreatedAt).Take(limit).ToList();
        }
        else
        {
            return await query.OrderByDescending(x => x.CreatedAt).Take(limit).ToListAsync(cancellationToken);
        }
    }

    public async Task<int> GetCountByStatusAsync(string moduleName, string status, CancellationToken cancellationToken)
    {
        return await _dbContext.Set<OutboxMessage>().CountAsync(x => x.ModuleName == moduleName && x.Status == status, cancellationToken);
    }

    public async Task DeleteOldMessagesAsync(string moduleName, DateTimeOffset olderThan, CancellationToken cancellationToken)
    {
        if (_dbContext.Database.ProviderName == "Microsoft.EntityFrameworkCore.Sqlite")
        {
            var messages = await _dbContext.Set<OutboxMessage>()
                .Where(x => x.ModuleName == moduleName && x.Status == "Processed")
                .ToListAsync(cancellationToken);

            var toDelete = messages.Where(x => x.CreatedAt < olderThan).ToList();

            if (toDelete.Count > 0)
            {
                _dbContext.Set<OutboxMessage>().RemoveRange(toDelete);
                await _dbContext.SaveChangesAsync(cancellationToken);
            }
        }
        else
        {
            await _dbContext.Set<OutboxMessage>()
                .Where(x => x.ModuleName == moduleName && x.Status == "Processed" && x.CreatedAt < olderThan)
                .ExecuteDeleteAsync(cancellationToken);
        }
    }
}

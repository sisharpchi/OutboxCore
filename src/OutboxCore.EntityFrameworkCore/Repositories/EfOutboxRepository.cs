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
        string workerId,
        TimeSpan lockDuration,
        int batchSize,
        CancellationToken cancellationToken)
    {
        var tableName = _dbContext.Model.FindEntityType(typeof(OutboxMessage))?.GetTableName() ?? "OutboxMessages";
        var sql = _dialect.GetLockMessagesSql(tableName, batchSize);

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

            return await _dbContext.Set<OutboxMessage>()
                .FromSqlRaw(sql, lockedUntilParam, workerIdParam, nowParam)
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
        Guid messageId,
        string status,
        DateTimeOffset? processedAt,
        string? error,
        int retryCount,
        CancellationToken cancellationToken)
    {
        var message = await _dbContext.Set<OutboxMessage>().FindAsync(new object[] { messageId }, cancellationToken);
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

    public async Task DeleteMessageAsync(Guid messageId, CancellationToken cancellationToken)
    {
        var message = await _dbContext.Set<OutboxMessage>().FindAsync(new object[] { messageId }, cancellationToken);
        if (message != null)
        {
            _dbContext.Set<OutboxMessage>().Remove(message);
            await _dbContext.SaveChangesAsync(cancellationToken);
        }
    }
}

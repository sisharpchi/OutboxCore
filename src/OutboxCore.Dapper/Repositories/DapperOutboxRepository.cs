using System;
using System.Collections.Generic;
using System.Data;
using System.Threading;
using System.Threading.Tasks;
using Dapper;
using OutboxCore.Abstractions;
using OutboxCore.Models;

namespace OutboxCore.Dapper.Repositories;

public class DapperOutboxRepository : IOutboxRepository
{
    private readonly Func<System.Data.Common.DbConnection> _connectionFactory;
    private readonly ISqlDialect _dialect;

    public DapperOutboxRepository(Func<System.Data.Common.DbConnection> connectionFactory, ISqlDialect dialect)
    {
        _connectionFactory = connectionFactory;
        _dialect = dialect;
    }

    public async Task<IReadOnlyList<OutboxMessage>> LockMessagesAsync(
        string moduleName,
        string workerId,
        TimeSpan lockDuration,
        int batchSize,
        CancellationToken cancellationToken)
    {
        var tableName = "OutboxMessages";
        var sql = _dialect.GetLockMessagesSql(null, tableName, batchSize);

        var now = DateTimeOffset.UtcNow;
        var lockedUntil = now.Add(lockDuration);

        using var connection = _connectionFactory();
        if (connection.State == ConnectionState.Closed)
        {
            await connection.OpenAsync(cancellationToken);
        }

        var parameters = new
        {
            LockedUntil = lockedUntil,
            WorkerId = workerId,
            Now = now,
            ModuleName = moduleName
        };

        var messages = await connection.QueryAsync<OutboxMessage>(
            new CommandDefinition(sql, parameters, cancellationToken: cancellationToken));

        return messages.AsList();
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
        var sql = @"
            UPDATE ""OutboxMessages""
            SET ""Status"" = @Status,
                ""ProcessedAt"" = @ProcessedAt,
                ""Error"" = @Error,
                ""RetryCount"" = @RetryCount,
                ""LockedUntil"" = NULL,
                ""WorkerId"" = NULL
            WHERE ""Id"" = @Id AND ""ModuleName"" = @ModuleName;";

        using var connection = _connectionFactory();
        if (connection.State == ConnectionState.Closed)
        {
            await connection.OpenAsync(cancellationToken);
        }

        await connection.ExecuteAsync(new CommandDefinition(sql, new
        {
            Status = status,
            ProcessedAt = processedAt,
            Error = error,
            RetryCount = retryCount,
            Id = messageId,
            ModuleName = moduleName
        }, cancellationToken: cancellationToken));
    }

    public async Task DeleteMessageAsync(string moduleName, Guid messageId, CancellationToken cancellationToken)
    {
        var sql = @"DELETE FROM ""OutboxMessages"" WHERE ""Id"" = @Id AND ""ModuleName"" = @ModuleName;";

        using var connection = _connectionFactory();
        if (connection.State == ConnectionState.Closed)
        {
            await connection.OpenAsync(cancellationToken);
        }

        await connection.ExecuteAsync(new CommandDefinition(sql, new { Id = messageId, ModuleName = moduleName }, cancellationToken: cancellationToken));
    }

    public async Task<IReadOnlyList<OutboxMessage>> GetMessagesAsync(string moduleName, string? status, int limit, CancellationToken cancellationToken)
    {
        var sql = @"SELECT * FROM ""OutboxMessages"" WHERE ""ModuleName"" = @ModuleName " + 
                  (string.IsNullOrEmpty(status) ? "" : @"AND ""Status"" = @Status ") + 
                  @"ORDER BY ""CreatedAt"" DESC LIMIT @Limit;";
        using var connection = _connectionFactory();
        if (connection.State == ConnectionState.Closed)
        {
            await connection.OpenAsync(cancellationToken);
        }
        var messages = await connection.QueryAsync<OutboxMessage>(new CommandDefinition(sql, new { ModuleName = moduleName, Status = status, Limit = limit }, cancellationToken: cancellationToken));
        return messages.AsList();
    }

    public async Task<int> GetCountByStatusAsync(string moduleName, string status, CancellationToken cancellationToken)
    {
        var sql = @"SELECT COUNT(1) FROM ""OutboxMessages"" WHERE ""ModuleName"" = @ModuleName AND ""Status"" = @Status;";
        using var connection = _connectionFactory();
        if (connection.State == ConnectionState.Closed)
        {
            await connection.OpenAsync(cancellationToken);
        }
        return await connection.ExecuteScalarAsync<int>(new CommandDefinition(sql, new { ModuleName = moduleName, Status = status }, cancellationToken: cancellationToken));
    }

    public async Task DeleteOldMessagesAsync(string moduleName, DateTimeOffset olderThan, CancellationToken cancellationToken)
    {
        var sql = @"DELETE FROM ""OutboxMessages"" WHERE ""ModuleName"" = @ModuleName AND ""Status"" = 'Processed' AND ""CreatedAt"" < @OlderThan;";
        using var connection = _connectionFactory();
        if (connection.State == ConnectionState.Closed)
        {
            await connection.OpenAsync(cancellationToken);
        }
        await connection.ExecuteAsync(new CommandDefinition(sql, new { ModuleName = moduleName, OlderThan = olderThan }, cancellationToken: cancellationToken));
    }
}

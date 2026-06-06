using System;
using System.Collections.Generic;
using System.Data;
using System.Threading;
using System.Threading.Tasks;
using Dapper;
using OutboxCore.Abstractions;
using OutboxCore.Models;

namespace OutboxCore.Dapper.Repositories;

public class DapperInboxProcessor : IInboxProcessor
{
    private readonly Func<System.Data.Common.DbConnection> _connectionFactory;

    public DapperInboxProcessor(Func<System.Data.Common.DbConnection> connectionFactory)
    {
        _connectionFactory = connectionFactory;
    }

    public async Task<bool> HasBeenProcessedAsync(string moduleName, Guid messageId, CancellationToken cancellationToken = default)
    {
        var sql = @"SELECT COUNT(1) FROM ""InboxMessages"" WHERE ""Id"" = @Id AND ""ModuleName"" = @ModuleName AND ""Status"" = 'Processed';";
        using var connection = _connectionFactory();
        if (connection.State == ConnectionState.Closed)
        {
            await connection.OpenAsync(cancellationToken);
        }

        var count = await connection.ExecuteScalarAsync<int>(new CommandDefinition(sql, new { Id = messageId, ModuleName = moduleName }, cancellationToken: cancellationToken));
        return count > 0;
    }

    public async Task TrackMessageAsync(string moduleName, Guid messageId, string messageType, CancellationToken cancellationToken = default)
    {
        var sql = @"
            INSERT INTO ""InboxMessages"" (""Id"", ""ModuleName"", ""MessageType"", ""ReceivedAt"", ""Status"")
            VALUES (@Id, @ModuleName, @MessageType, @ReceivedAt, 'Pending');";

        using var connection = _connectionFactory();
        if (connection.State == ConnectionState.Closed)
        {
            await connection.OpenAsync(cancellationToken);
        }

        await connection.ExecuteAsync(new CommandDefinition(sql, new
        {
            Id = messageId,
            ModuleName = moduleName,
            MessageType = messageType,
            ReceivedAt = DateTimeOffset.UtcNow
        }, cancellationToken: cancellationToken));
    }

    public async Task MarkAsProcessedAsync(string moduleName, Guid messageId, CancellationToken cancellationToken = default)
    {
        var sql = @"
            UPDATE ""InboxMessages""
            SET ""Status"" = 'Processed',
                ""ProcessedAt"" = @ProcessedAt
            WHERE ""Id"" = @Id AND ""ModuleName"" = @ModuleName;";

        using var connection = _connectionFactory();
        if (connection.State == ConnectionState.Closed)
        {
            await connection.OpenAsync(cancellationToken);
        }

        await connection.ExecuteAsync(new CommandDefinition(sql, new
        {
            Id = messageId,
            ModuleName = moduleName,
            ProcessedAt = DateTimeOffset.UtcNow
        }, cancellationToken: cancellationToken));
    }

    public async Task MarkAsFailedAsync(string moduleName, Guid messageId, string error, CancellationToken cancellationToken = default)
    {
        var sql = @"
            UPDATE ""InboxMessages""
            SET ""Status"" = 'Failed',
                ""Error"" = @Error
            WHERE ""Id"" = @Id AND ""ModuleName"" = @ModuleName;";

        using var connection = _connectionFactory();
        if (connection.State == ConnectionState.Closed)
        {
            await connection.OpenAsync(cancellationToken);
        }

        await connection.ExecuteAsync(new CommandDefinition(sql, new
        {
            Id = messageId,
            ModuleName = moduleName,
            Error = error
        }, cancellationToken: cancellationToken));
    }

    public async Task<IReadOnlyList<InboxMessage>> GetMessagesAsync(string moduleName, string? status, int limit, CancellationToken cancellationToken = default)
    {
        var sql = @"SELECT * FROM ""InboxMessages"" WHERE ""ModuleName"" = @ModuleName " + 
                  (string.IsNullOrEmpty(status) ? "" : @"AND ""Status"" = @Status ") + 
                  @"ORDER BY ""ReceivedAt"" DESC LIMIT @Limit;";
        using var connection = _connectionFactory();
        if (connection.State == ConnectionState.Closed)
        {
            await connection.OpenAsync(cancellationToken);
        }
        var messages = await connection.QueryAsync<InboxMessage>(new CommandDefinition(sql, new { ModuleName = moduleName, Status = status, Limit = limit }, cancellationToken: cancellationToken));
        return messages.AsList();
    }

    public async Task<int> GetCountByStatusAsync(string moduleName, string status, CancellationToken cancellationToken = default)
    {
        var sql = @"SELECT COUNT(1) FROM ""InboxMessages"" WHERE ""ModuleName"" = @ModuleName AND ""Status"" = @Status;";
        using var connection = _connectionFactory();
        if (connection.State == ConnectionState.Closed)
        {
            await connection.OpenAsync(cancellationToken);
        }
        return await connection.ExecuteScalarAsync<int>(new CommandDefinition(sql, new { ModuleName = moduleName, Status = status }, cancellationToken: cancellationToken));
    }

    public async Task DeleteOldMessagesAsync(string moduleName, DateTimeOffset olderThan, CancellationToken cancellationToken = default)
    {
        var sql = @"DELETE FROM ""InboxMessages"" WHERE ""ModuleName"" = @ModuleName AND (""Status"" = 'Processed' OR ""Status"" = 'Failed') AND ""ReceivedAt"" < @OlderThan;";
        using var connection = _connectionFactory();
        if (connection.State == ConnectionState.Closed)
        {
            await connection.OpenAsync(cancellationToken);
        }
        await connection.ExecuteAsync(new CommandDefinition(sql, new { ModuleName = moduleName, OlderThan = olderThan }, cancellationToken: cancellationToken));
    }
}

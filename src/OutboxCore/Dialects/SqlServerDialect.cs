using OutboxCore.Abstractions;

namespace OutboxCore.Dialects;

public class SqlServerDialect : ISqlDialect
{
    public string GetLockMessagesSql(string tableName, int batchSize)
    {
        return $@"
            UPDATE [{tableName}]
            SET [Status] = 'Processing',
                [LockedUntil] = @LockedUntil,
                [WorkerId] = @WorkerId
            OUTPUT INSERTED.[Id], INSERTED.[MessageType], INSERTED.[Content], INSERTED.[CreatedAt], INSERTED.[ProcessedAt], INSERTED.[Status], INSERTED.[LockedUntil], INSERTED.[WorkerId], INSERTED.[Error], INSERTED.[RetryCount]
            WHERE [Id] IN (
                SELECT TOP ({batchSize}) [Id]
                FROM [{tableName}] WITH (UPDLOCK, READPAST)
                WHERE [Status] = 'Pending' OR ([Status] = 'Processing' AND [LockedUntil] < @Now)
                ORDER BY [CreatedAt]
            );";
    }
}

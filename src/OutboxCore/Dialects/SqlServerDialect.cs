using OutboxCore.Abstractions;

namespace OutboxCore.Dialects;

public class SqlServerDialect : ISqlDialect
{
    public string GetLockMessagesSql(string? schema, string tableName, int batchSize)
    {
        var tableIdentifier = string.IsNullOrEmpty(schema) ? $"[{tableName}]" : $"[{schema}].[{tableName}]";
        return $@"
            UPDATE {tableIdentifier}
            SET [Status] = 'Processing',
                [LockedUntil] = @LockedUntil,
                [WorkerId] = @WorkerId
            OUTPUT INSERTED.[Id], INSERTED.[ModuleName], INSERTED.[MessageType], INSERTED.[Content], INSERTED.[CreatedAt], INSERTED.[ProcessedAt], INSERTED.[Status], INSERTED.[LockedUntil], INSERTED.[WorkerId], INSERTED.[Error], INSERTED.[RetryCount]
            WHERE [Id] IN (
                SELECT TOP ({batchSize}) [Id]
                FROM {tableIdentifier} WITH (UPDLOCK, READPAST)
                WHERE [ModuleName] = @ModuleName 
                  AND ([Status] = 'Pending' OR ([Status] = 'Processing' AND [LockedUntil] < @Now))
                ORDER BY [CreatedAt]
            );";
    }
}

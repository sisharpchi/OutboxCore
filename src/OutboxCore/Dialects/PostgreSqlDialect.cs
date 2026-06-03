using OutboxCore.Abstractions;

namespace OutboxCore.Dialects;

public class PostgreSqlDialect : ISqlDialect
{
    public string GetLockMessagesSql(string tableName, int batchSize)
    {
        return $@"
            UPDATE ""{tableName}""
            SET ""Status"" = 'Processing',
                ""LockedUntil"" = @LockedUntil,
                ""WorkerId"" = @WorkerId
            WHERE ""Id"" IN (
                SELECT ""Id""
                FROM ""{tableName}""
                WHERE ""Status"" = 'Pending' OR (""Status"" = 'Processing' AND ""LockedUntil"" < @Now)
                ORDER BY ""CreatedAt""
                LIMIT {batchSize}
                FOR UPDATE SKIP LOCKED
            )
            RETURNING ""Id"", ""MessageType"", ""Content"", ""CreatedAt"", ""ProcessedAt"", ""Status"", ""LockedUntil"", ""WorkerId"", ""Error"", ""RetryCount"";";
    }
}

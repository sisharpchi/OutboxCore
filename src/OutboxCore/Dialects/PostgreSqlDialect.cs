using OutboxCore.Abstractions;

namespace OutboxCore.Dialects;

public class PostgreSqlDialect : ISqlDialect
{
    public string GetLockMessagesSql(string? schema, string tableName, int batchSize)
    {
        var tableIdentifier = string.IsNullOrEmpty(schema) ? $@"""{tableName}""" : $@"""{schema}"".""{tableName}""";
        return $@"
            UPDATE {tableIdentifier}
            SET ""Status"" = 'Processing',
                ""LockedUntil"" = @LockedUntil,
                ""WorkerId"" = @WorkerId
            WHERE ""Id"" IN (
                SELECT ""Id""
                FROM {tableIdentifier}
                WHERE ""ModuleName"" = @ModuleName 
                  AND (""Status"" = 'Pending' OR (""Status"" = 'Processing' AND ""LockedUntil"" < @Now))
                ORDER BY ""CreatedAt""
                LIMIT {batchSize}
                FOR UPDATE SKIP LOCKED
            )
            RETURNING ""Id"", ""ModuleName"", ""MessageType"", ""Content"", ""CreatedAt"", ""ProcessedAt"", ""Status"", ""LockedUntil"", ""WorkerId"", ""Error"", ""RetryCount"";";
    }
}

namespace OutboxCore.Abstractions;

public interface ISqlDialect
{
    string GetLockMessagesSql(string? schema, string tableName, int batchSize);
}

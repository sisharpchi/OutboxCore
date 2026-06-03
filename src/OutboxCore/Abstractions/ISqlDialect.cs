namespace OutboxCore.Abstractions;

public interface ISqlDialect
{
    string GetLockMessagesSql(string tableName, int batchSize);
}

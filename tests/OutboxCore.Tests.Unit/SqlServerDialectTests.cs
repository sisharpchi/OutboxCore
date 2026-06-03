using OutboxCore.Dialects;
using Xunit;

namespace OutboxCore.Tests.Unit;

public class SqlServerDialectTests
{
    [Fact]
    public void GetLockMessagesSql_ShouldReturnValidSqlWithTableNameAndBatchSize()
    {
        // Arrange
        var dialect = new SqlServerDialect();
        var tableName = "MyOutboxTable";
        var batchSize = 35;

        // Act
        var sql = dialect.GetLockMessagesSql(tableName, batchSize);

        // Assert
        Assert.Contains($"UPDATE [{tableName}]", sql);
        Assert.Contains($"TOP ({batchSize})", sql);
        Assert.Contains("WITH (UPDLOCK, READPAST)", sql);
    }
}

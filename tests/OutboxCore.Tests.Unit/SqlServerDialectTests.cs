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
        var sql = dialect.GetLockMessagesSql(null, tableName, batchSize);

        // Assert
        Assert.Contains($"UPDATE [{tableName}]", sql);
        Assert.Contains($"TOP ({batchSize})", sql);
        Assert.Contains("WITH (UPDLOCK, READPAST)", sql);
        Assert.Contains("[ModuleName] = @ModuleName", sql);
    }

    [Fact]
    public void GetLockMessagesSql_WithSchema_ShouldReturnSchemaQualifiedTable()
    {
        // Arrange
        var dialect = new SqlServerDialect();
        var schema = "custom";
        var tableName = "MyOutboxTable";
        var batchSize = 35;

        // Act
        var sql = dialect.GetLockMessagesSql(schema, tableName, batchSize);

        // Assert
        Assert.Contains($"UPDATE [{schema}].[{tableName}]", sql);
        Assert.Contains($"FROM [{schema}].[{tableName}] WITH (UPDLOCK, READPAST)", sql);
    }
}

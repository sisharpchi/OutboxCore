using OutboxCore.Dialects;
using Xunit;

namespace OutboxCore.Tests.Unit;

public class PostgreSqlDialectTests
{
    [Fact]
    public void GetLockMessagesSql_ShouldReturnValidSqlWithTableNameAndBatchSize()
    {
        // Arrange
        var dialect = new PostgreSqlDialect();
        var tableName = "MyOutboxTable";
        var batchSize = 42;

        // Act
        var sql = dialect.GetLockMessagesSql(null, tableName, batchSize);

        // Assert
        Assert.Contains($@"UPDATE ""{tableName}""", sql);
        Assert.Contains($"LIMIT {batchSize}", sql);
        Assert.Contains("FOR UPDATE SKIP LOCKED", sql);
        Assert.Contains(@"""ModuleName"" = @ModuleName", sql);
    }

    [Fact]
    public void GetLockMessagesSql_WithSchema_ShouldReturnSchemaQualifiedTable()
    {
        // Arrange
        var dialect = new PostgreSqlDialect();
        var schema = "custom";
        var tableName = "MyOutboxTable";
        var batchSize = 42;

        // Act
        var sql = dialect.GetLockMessagesSql(schema, tableName, batchSize);

        // Assert
        Assert.Contains($@"UPDATE ""{schema}"".""{tableName}""", sql);
        Assert.Contains($@"FROM ""{schema}"".""{tableName}""", sql);
    }
}

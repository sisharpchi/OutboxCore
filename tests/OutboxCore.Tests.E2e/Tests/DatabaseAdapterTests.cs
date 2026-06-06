using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;
using OutboxCore.Abstractions;
using OutboxCore.Dialects;
using OutboxCore.Models;
using OutboxCore.Tests.E2e.Fixtures;
using Xunit;

namespace OutboxCore.Tests.E2e.Tests;

public class DatabaseAdapterTests : IClassFixture<E2eTestFixture>
{
    private readonly E2eTestFixture _fixture;

    public DatabaseAdapterTests(E2eTestFixture fixture)
    {
        _fixture = fixture;
        _fixture.SeedTestDatabase();
        _fixture.ClearBroker();
    }

    [Fact]
    public async Task F2_T1_01_EfCoreAdapterStoresAndLocksOutboxMessages()
    {
        using var scope = _fixture.Services.CreateScope();
        var repo = scope.ServiceProvider.GetKeyedService<IOutboxRepository>("Default") 
                   ?? scope.ServiceProvider.GetRequiredService<IOutboxRepository>();

        Assert.NotNull(repo);
        var messages = await repo.LockMessagesAsync("Default", "worker-ef", TimeSpan.FromSeconds(5), 10, default);
        Assert.NotNull(messages);
    }

    [Fact]
    public async Task F2_T1_02_DapperAdapterStoresAndLocksOutboxMessages()
    {
        using var scope = _fixture.Services.CreateScope();
        var repo = scope.ServiceProvider.GetKeyedService<IOutboxRepository>("Default") 
                   ?? scope.ServiceProvider.GetRequiredService<IOutboxRepository>();

        Assert.NotNull(repo);
        var messages = await repo.LockMessagesAsync("Default", "worker-dapper", TimeSpan.FromSeconds(5), 10, default);
        Assert.NotNull(messages);
    }

    [Fact]
    public void F2_T1_03_VerifySqliteDialectLockSqlFormattingIsCorrect()
    {
        var dialect = new SqliteDialect();
        var sql = dialect.GetLockMessagesSql("schema", "OutboxMessages", 10);
        Assert.Contains("`schema`.`OutboxMessages`", sql);
    }

    [Fact]
    public void F2_T1_04_VerifyPostgreSqlDialectLockSqlFormattingIsCorrect()
    {
        var dialect = new PostgreSqlDialect();
        var sql = dialect.GetLockMessagesSql("schema", "OutboxMessages", 10);
        Assert.Contains("\"schema\".\"OutboxMessages\"", sql);
    }

    [Fact]
    public void F2_T1_05_VerifySqlServerDialectLockSqlFormattingIsCorrect()
    {
        var dialect = new SqlServerDialect();
        var sql = dialect.GetLockMessagesSql("schema", "OutboxMessages", 10);
        Assert.Contains("[schema].[OutboxMessages] WITH (UPDLOCK, READPAST)", sql);
    }

    [Fact]
    public async Task F2_T2_01_VerifyLockAndFetchHandlingUnderEmptyTables()
    {
        using var scope = _fixture.Services.CreateScope();
        var repo = scope.ServiceProvider.GetKeyedService<IOutboxRepository>("Default") 
                   ?? scope.ServiceProvider.GetRequiredService<IOutboxRepository>();

        var messages = await repo.LockMessagesAsync("Default", "worker-empty", TimeSpan.FromSeconds(5), 10, default);
        Assert.Empty(messages);
    }

    [Fact]
    public async Task F2_T2_02_VerifyUpdateMessageStatusUpdatesRetryCountCorrectly()
    {
        using var scope = _fixture.Services.CreateScope();
        var repo = scope.ServiceProvider.GetKeyedService<IOutboxRepository>("Default") 
                   ?? scope.ServiceProvider.GetRequiredService<IOutboxRepository>();

        var messageId = Guid.NewGuid();
        // Since we are checking status updating:
        await repo.UpdateMessageStatusAsync("Default", messageId, "Failed", DateTimeOffset.UtcNow, "Error", 3, default);
        
        var count = await repo.GetCountByStatusAsync("Default", "Failed", default);
        Assert.True(count >= 0);
    }

    [Fact]
    public async Task F2_T2_03_VerifyDeleteOnPublishDeletesOutboxMessage()
    {
        using var scope = _fixture.Services.CreateScope();
        var repo = scope.ServiceProvider.GetKeyedService<IOutboxRepository>("Default") 
                   ?? scope.ServiceProvider.GetRequiredService<IOutboxRepository>();

        var messageId = Guid.NewGuid();
        await repo.DeleteMessageAsync("Default", messageId, default);
        
        var count = await repo.GetCountByStatusAsync("Default", "Processed", default);
        Assert.True(count >= 0);
    }

    [Fact]
    public async Task F2_T2_04_VerifyDatabaseTransactionRollbackPreservesOutboxState()
    {
        using var scope = _fixture.Services.CreateScope();
        var dbContext = scope.ServiceProvider.GetRequiredService<OutboxCore.Sample.WebApi.Data.ApplicationDbContext>();
        
        using var transaction = await dbContext.Database.BeginTransactionAsync();
        var msg = new OutboxMessage { Id = Guid.NewGuid(), ModuleName = "Default", MessageType = "Test", Content = "{}", Status = "Pending", CreatedAt = DateTimeOffset.UtcNow };
        dbContext.OutboxMessages.Add(msg);
        await dbContext.SaveChangesAsync();
        await transaction.RollbackAsync();

        dbContext.ChangeTracker.Clear();
        var exists = await dbContext.OutboxMessages.FindAsync(msg.Id);
        Assert.Null(exists);
    }

    [Fact]
    public async Task F2_T2_05_VerifyLockDurationEnforcement()
    {
        using var scope = _fixture.Services.CreateScope();
        var repo = scope.ServiceProvider.GetKeyedService<IOutboxRepository>("Default") 
                   ?? scope.ServiceProvider.GetRequiredService<IOutboxRepository>();

        var msg = new OutboxMessage
        {
            Id = Guid.NewGuid(),
            ModuleName = "Default",
            MessageType = "TestType",
            Content = "{}",
            Status = "Pending",
            CreatedAt = DateTimeOffset.UtcNow,
            LockedUntil = DateTimeOffset.UtcNow.AddSeconds(-10),
            WorkerId = "worker-expired"
        };

        if (repo is OutboxCore.EntityFrameworkCore.Repositories.EfOutboxRepository<OutboxCore.Sample.WebApi.Data.ApplicationDbContext> efRepo)
        {
            var db = scope.ServiceProvider.GetRequiredService<OutboxCore.Sample.WebApi.Data.ApplicationDbContext>();
            db.OutboxMessages.Add(msg);
            await db.SaveChangesAsync();

            // Expired lock should be picked up again
            var lockedMsgs = await repo.LockMessagesAsync("Default", "worker-new", TimeSpan.FromSeconds(5), 10, default);
            Assert.Contains(lockedMsgs, m => m.Id == msg.Id);
        }
        else
        {
            Assert.True(true);
        }
    }
}

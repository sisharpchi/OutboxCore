using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;
using OutboxCore.Abstractions;
using OutboxCore.Models;
using OutboxCore.Tests.E2e.Fixtures;
using Xunit;

namespace OutboxCore.Tests.E2e.Tests;

public class CleanupServiceTests : IClassFixture<E2eTestFixture>
{
    private readonly E2eTestFixture _fixture;

    public CleanupServiceTests(E2eTestFixture fixture)
    {
        _fixture = fixture;
        _fixture.SeedTestDatabase();
    }

    [Fact]
    public async Task EFCore_DeleteOldMessages_ShouldPruneCorrectRecordsOnly()
    {
        using var scope = _fixture.Services.CreateScope();
        var db = scope.ServiceProvider.GetRequiredService<OutboxCore.Sample.WebApi.Data.ApplicationDbContext>();
        var outboxRepo = scope.ServiceProvider.GetKeyedService<IOutboxRepository>("Orders") 
                         ?? scope.ServiceProvider.GetRequiredService<IOutboxRepository>();
        var inboxProcessor = scope.ServiceProvider.GetKeyedService<IInboxProcessor>("Orders") 
                             ?? scope.ServiceProvider.GetRequiredService<IInboxProcessor>();

        // Clean any existing messages first
        db.OutboxMessages.RemoveRange(db.OutboxMessages.Where(m => m.ModuleName == "Orders"));
        db.InboxMessages.RemoveRange(db.InboxMessages.Where(m => m.ModuleName == "Orders"));
        await db.SaveChangesAsync();

        var now = DateTimeOffset.UtcNow;
        var oldProcessedOutboxId = Guid.NewGuid();
        var newProcessedOutboxId = Guid.NewGuid();
        var oldPendingOutboxId = Guid.NewGuid();

        var oldProcessedInboxId = Guid.NewGuid();
        var newProcessedInboxId = Guid.NewGuid();
        var oldFailedInboxId = Guid.NewGuid();

        // 1. Seed Outbox Messages
        var messages = new List<OutboxMessage>
        {
            // Old processed -> Should be deleted
            new OutboxMessage { Id = oldProcessedOutboxId, ModuleName = "Orders", MessageType = "Test", Content = "{}", Status = "Processed", CreatedAt = now.AddHours(-26) },
            // New processed -> Should NOT be deleted
            new OutboxMessage { Id = newProcessedOutboxId, ModuleName = "Orders", MessageType = "Test", Content = "{}", Status = "Processed", CreatedAt = now.AddMinutes(-10) },
            // Old pending -> Should NOT be deleted
            new OutboxMessage { Id = oldPendingOutboxId, ModuleName = "Orders", MessageType = "Test", Content = "{}", Status = "Pending", CreatedAt = now.AddHours(-26) }
        };
        db.OutboxMessages.AddRange(messages);

        // 2. Seed Inbox Messages
        var inboxMessages = new List<InboxMessage>
        {
            // Old processed -> Should be deleted
            new InboxMessage { Id = oldProcessedInboxId, ModuleName = "Orders", MessageType = "Test", Status = "Processed", ReceivedAt = now.AddDays(-8) },
            // New processed -> Should NOT be deleted
            new InboxMessage { Id = newProcessedInboxId, ModuleName = "Orders", MessageType = "Test", Status = "Processed", ReceivedAt = now.AddMinutes(-15) },
            // Old failed -> Should be deleted (retention is 7 days)
            new InboxMessage { Id = oldFailedInboxId, ModuleName = "Orders", MessageType = "Test", Status = "Failed", ReceivedAt = now.AddDays(-8) }
        };
        db.InboxMessages.AddRange(inboxMessages);

        await db.SaveChangesAsync();

        // Act
        var outboxOlderThan = now.AddHours(-24);
        var inboxOlderThan = now.AddDays(-7);

        await outboxRepo.DeleteOldMessagesAsync("Orders", outboxOlderThan, default);
        await inboxProcessor.DeleteOldMessagesAsync("Orders", inboxOlderThan, default);

        // Assert
        db.ChangeTracker.Clear();

        var remainingOutbox = db.OutboxMessages.Where(m => m.ModuleName == "Orders").ToList();
        Assert.Equal(2, remainingOutbox.Count);
        Assert.Contains(remainingOutbox, m => m.Id == newProcessedOutboxId);
        Assert.Contains(remainingOutbox, m => m.Id == oldPendingOutboxId);
        Assert.DoesNotContain(remainingOutbox, m => m.Id == oldProcessedOutboxId);

        var remainingInbox = db.InboxMessages.Where(m => m.ModuleName == "Orders").ToList();
        Assert.Single(remainingInbox);
        Assert.Contains(remainingInbox, m => m.Id == newProcessedInboxId);
        Assert.DoesNotContain(remainingInbox, m => m.Id == oldProcessedInboxId);
        Assert.DoesNotContain(remainingInbox, m => m.Id == oldFailedInboxId);
    }

    [Fact]
    public async Task Dapper_DeleteOldMessages_ShouldPruneCorrectRecordsOnly()
    {
        // Billing module is registered with Dapper
        using var scope = _fixture.Services.CreateScope();
        var outboxRepo = scope.ServiceProvider.GetKeyedService<IOutboxRepository>("Billing");
        var inboxProcessor = scope.ServiceProvider.GetKeyedService<IInboxProcessor>("Billing");

        Assert.NotNull(outboxRepo);
        Assert.NotNull(inboxProcessor);

        // Seed some records into the DB using EF Core first (so we can assert Dapper deletes them)
        var db = scope.ServiceProvider.GetRequiredService<OutboxCore.Sample.WebApi.Data.ApplicationDbContext>();
        db.OutboxMessages.RemoveRange(db.OutboxMessages.Where(m => m.ModuleName == "Billing"));
        db.InboxMessages.RemoveRange(db.InboxMessages.Where(m => m.ModuleName == "Billing"));
        await db.SaveChangesAsync();

        var now = DateTimeOffset.UtcNow;
        var oldProcessedOutboxId = Guid.NewGuid();
        var newProcessedOutboxId = Guid.NewGuid();
        var oldProcessedInboxId = Guid.NewGuid();

        db.OutboxMessages.AddRange(
            new OutboxMessage { Id = oldProcessedOutboxId, ModuleName = "Billing", MessageType = "Test", Content = "{}", Status = "Processed", CreatedAt = now.AddHours(-26) },
            new OutboxMessage { Id = newProcessedOutboxId, ModuleName = "Billing", MessageType = "Test", Content = "{}", Status = "Processed", CreatedAt = now.AddMinutes(-10) }
        );

        db.InboxMessages.AddRange(
            new InboxMessage { Id = oldProcessedInboxId, ModuleName = "Billing", MessageType = "Test", Status = "Processed", ReceivedAt = now.AddDays(-8) }
        );

        await db.SaveChangesAsync();

        // Act
        var outboxOlderThan = now.AddHours(-24);
        var inboxOlderThan = now.AddDays(-7);

        await outboxRepo.DeleteOldMessagesAsync("Billing", outboxOlderThan, default);
        await inboxProcessor.DeleteOldMessagesAsync("Billing", inboxOlderThan, default);

        // Assert
        db.ChangeTracker.Clear();

        var remainingOutbox = db.OutboxMessages.Where(m => m.ModuleName == "Billing").ToList();
        Assert.Single(remainingOutbox);
        Assert.Equal(newProcessedOutboxId, remainingOutbox[0].Id);

        var remainingInbox = db.InboxMessages.Where(m => m.ModuleName == "Billing").ToList();
        Assert.Empty(remainingInbox);
    }
}

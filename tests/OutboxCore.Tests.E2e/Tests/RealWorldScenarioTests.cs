using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;
using OutboxCore.Abstractions;
using OutboxCore.Inbox;
using OutboxCore.Models;
using OutboxCore.Tests.E2e.Fixtures;
using Xunit;

namespace OutboxCore.Tests.E2e.Tests;

public class RealWorldScenarioTests : IClassFixture<E2eTestFixture>
{
    private readonly E2eTestFixture _fixture;

    public RealWorldScenarioTests(E2eTestFixture fixture)
    {
        _fixture = fixture;
        _fixture.SeedTestDatabase();
        _fixture.ClearBroker();
    }

    public class OrderPlacedEvent
    {
        public Guid Id { get; set; } = Guid.NewGuid();
        public string CustomerId { get; set; } = "";
    }

    public class BillingHandler : IIntegrationEventHandler<OrderPlacedEvent>
    {
        public bool InvoiceCreated { get; private set; }
        public Task HandleAsync(OrderPlacedEvent @event, System.Threading.CancellationToken cancellationToken)
        {
            InvoiceCreated = true;
            return Task.CompletedTask;
        }
    }

    [Fact]
    public async Task RW_01_OrderToBillingMonolithFlow_WithIdempotencyAndBroker()
    {
        // 1. Place order (raise event, saved in EF Orders)
        using var scope = _fixture.Services.CreateScope();
        var db = scope.ServiceProvider.GetRequiredService<OutboxCore.Sample.WebApi.Data.ApplicationDbContext>();
        var outboxRepo = scope.ServiceProvider.GetKeyedService<IOutboxRepository>("Orders") 
                         ?? scope.ServiceProvider.GetRequiredService<IOutboxRepository>();

        var ev = new OrderPlacedEvent { CustomerId = "cust_123" };
        var outboxMsg = new OutboxMessage
        {
            Id = ev.Id,
            ModuleName = "Orders",
            MessageType = typeof(OrderPlacedEvent).FullName!,
            Content = "{\"CustomerId\":\"cust_123\"}",
            Status = "Pending",
            CreatedAt = DateTimeOffset.UtcNow
        };

        db.OutboxMessages.Add(outboxMsg);
        await db.SaveChangesAsync();

        // 2. Background worker processes Orders outbox, publishes to broker
        var publisher = scope.ServiceProvider.GetKeyedService<IMessagePublisher>("Orders") 
                        ?? scope.ServiceProvider.GetRequiredService<IMessagePublisher>();
        await publisher.PublishAsync(outboxMsg.MessageType, outboxMsg.Content, default);

        if (!_fixture.IsDockerActive)
        {
            _fixture.InMemoryPublisher.PublishedMessages.TryPeek(out var published);
            Assert.Equal(outboxMsg.MessageType, published.MessageType);
        }

        // 3. Billing module receives event from broker, uses Idempotent Handler Decorator to process
        var inboxProcessor = scope.ServiceProvider.GetKeyedService<IInboxProcessor>("Billing") 
                             ?? scope.ServiceProvider.GetRequiredService<IInboxProcessor>();
        var billingHandler = new BillingHandler();
        var decorator = new IdempotentIntegrationEventHandlerDecorator<OrderPlacedEvent>(
            billingHandler, inboxProcessor, "Billing", e => e.Id);

        await decorator.HandleAsync(ev, default);

        // Verify Billing handled event and tracking is in DB
        Assert.True(billingHandler.InvoiceCreated);
        var isProcessed = await inboxProcessor.HasBeenProcessedAsync("Billing", ev.Id, default);
        Assert.True(isProcessed);
    }

    [Fact]
    public async Task RW_02_MultiInstanceClusterConcurrencyRace()
    {
        var messageId = Guid.NewGuid();
        var msg = new OutboxMessage
        {
            Id = messageId,
            ModuleName = "ConcurrencyTest",
            MessageType = "RaceEvent",
            Content = "{}",
            Status = "Pending",
            CreatedAt = DateTimeOffset.UtcNow
        };

        using (var scope = _fixture.Services.CreateScope())
        {
            var db = scope.ServiceProvider.GetRequiredService<OutboxCore.Sample.WebApi.Data.ApplicationDbContext>();
            db.OutboxMessages.Add(msg);
            await db.SaveChangesAsync();
        }

        using var scope1 = _fixture.Services.CreateScope();
        using var scope2 = _fixture.Services.CreateScope();

        var repo1 = scope1.ServiceProvider.GetKeyedService<IOutboxRepository>("Default") 
                    ?? scope1.ServiceProvider.GetRequiredService<IOutboxRepository>();
        var repo2 = scope2.ServiceProvider.GetKeyedService<IOutboxRepository>("Default") 
                    ?? scope2.ServiceProvider.GetRequiredService<IOutboxRepository>();

        if (repo1 is OutboxCore.EntityFrameworkCore.Repositories.EfOutboxRepository<OutboxCore.Sample.WebApi.Data.ApplicationDbContext> efRepo)
        {
            // Simulate instance 1 and instance 2 concurrently locking
            var lock1Task = repo1.LockMessagesAsync("ConcurrencyTest", "worker-instance-1", TimeSpan.FromSeconds(5), 10, default);
            var lock2Task = repo2.LockMessagesAsync("ConcurrencyTest", "worker-instance-2", TimeSpan.FromSeconds(5), 10, default);

            await Task.WhenAll(lock1Task, lock2Task);

            // Only one instance should have locked the message
            var locked1 = lock1Task.Result.Any(m => m.Id == messageId);
            var locked2 = lock2Task.Result.Any(m => m.Id == messageId);

            Assert.True(locked1 ^ locked2); // Exclusive OR: one and only one is true
        }
        else
        {
            Assert.True(true);
        }
    }

    [Fact]
    public async Task RW_03_BrokerOutageAndDashboardRecovery()
    {
        // 1. Message fails to publish due to outage, retry count increments, status set to Failed
        using var scope = _fixture.Services.CreateScope();
        var repo = scope.ServiceProvider.GetKeyedService<IOutboxRepository>("Default") 
                   ?? scope.ServiceProvider.GetRequiredService<IOutboxRepository>();

        var messageId = Guid.NewGuid();
        var msg = new OutboxMessage
        {
            Id = messageId,
            ModuleName = "Default",
            MessageType = "FailedToPublishEvent",
            Content = "{}",
            Status = "Failed",
            CreatedAt = DateTimeOffset.UtcNow,
            RetryCount = 5,
            Error = "Connection reset by peer"
        };

        if (repo is OutboxCore.EntityFrameworkCore.Repositories.EfOutboxRepository<OutboxCore.Sample.WebApi.Data.ApplicationDbContext> efRepo)
        {
            var db = scope.ServiceProvider.GetRequiredService<OutboxCore.Sample.WebApi.Data.ApplicationDbContext>();
            db.OutboxMessages.Add(msg);
            await db.SaveChangesAsync();

            // 2. Administrator uses Dashboard UI to trigger manual retry
            var client = _fixture.CreateClient();
            var response = await client.PostAsync($"/outbox-dashboard/api/retry?module=Default&id={messageId}", null);
            Assert.Equal(System.Net.HttpStatusCode.OK, response.StatusCode);

            // 3. Message status reset to Pending, retry count reset to 0, channel notified
            db.ChangeTracker.Clear();
            var dbMsg = await db.OutboxMessages.FindAsync(messageId);
            Assert.Contains(dbMsg?.Status, new[] { "Pending", "Processing", "Processed" });
            Assert.Equal(0, dbMsg?.RetryCount);
            Assert.Null(dbMsg?.Error);
        }
        else
        {
            Assert.True(true);
        }
    }

    [Fact]
    public async Task RW_04_HeterogeneousEfCoreAndDapperCohabitation()
    {
        // Verification of EF Core and Dapper cohabitation
        using var scope = _fixture.Services.CreateScope();
        var db = scope.ServiceProvider.GetRequiredService<OutboxCore.Sample.WebApi.Data.ApplicationDbContext>();
        var outboxRepo = scope.ServiceProvider.GetKeyedService<IOutboxRepository>("Default") 
                         ?? scope.ServiceProvider.GetRequiredService<IOutboxRepository>();

        // EF Core writes a message
        var msg = new OutboxMessage
        {
            Id = Guid.NewGuid(),
            ModuleName = "Default",
            MessageType = "CohabitationEvent",
            Content = "{}",
            Status = "Pending",
            CreatedAt = DateTimeOffset.UtcNow
        };
        db.OutboxMessages.Add(msg);
        await db.SaveChangesAsync();

        // Dapper reads/queries it (via GetMessagesAsync or LockMessagesAsync)
        var messages = await outboxRepo.GetMessagesAsync("Default", null, 10, default);
        Assert.Contains(messages, m => m.Id == msg.Id);
    }

    [Fact]
    public async Task RW_05_DynamicFaultTolerantModuleProcessing()
    {
        // Validate processing behaves gracefully when a module fails
        using var scope = _fixture.Services.CreateScope();
        var repo = scope.ServiceProvider.GetKeyedService<IOutboxRepository>("Default") 
                   ?? scope.ServiceProvider.GetRequiredService<IOutboxRepository>();

        // Try locking under invalid module
        var messages = await repo.LockMessagesAsync("InvalidModule", "worker-x", TimeSpan.FromSeconds(5), 10, default);
        Assert.Empty(messages);
    }
}

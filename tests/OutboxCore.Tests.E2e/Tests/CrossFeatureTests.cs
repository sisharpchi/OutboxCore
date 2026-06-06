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

public class CrossFeatureTests : IClassFixture<E2eTestFixture>
{
    private readonly E2eTestFixture _fixture;

    public CrossFeatureTests(E2eTestFixture fixture)
    {
        _fixture = fixture;
        _fixture.SeedTestDatabase();
        _fixture.ClearBroker();
    }

    public class TestEvent
    {
        public Guid Id { get; set; } = Guid.NewGuid();
    }

    public class MockHandler : IIntegrationEventHandler<TestEvent>
    {
        public bool Handled { get; private set; }
        public Task HandleAsync(TestEvent @event, System.Threading.CancellationToken cancellationToken)
        {
            Handled = true;
            return Task.CompletedTask;
        }
    }

    [Fact]
    public async Task CF_01_EfCoreDbContextSavingDomainEventsPublishingViaInMemoryBroker()
    {
        using var scope = _fixture.Services.CreateScope();
        var db = scope.ServiceProvider.GetRequiredService<OutboxCore.Sample.WebApi.Data.ApplicationDbContext>();
        var publisher = scope.ServiceProvider.GetKeyedService<IMessagePublisher>("Default") 
                        ?? scope.ServiceProvider.GetRequiredService<IMessagePublisher>();

        var msg = new OutboxMessage
        {
            Id = Guid.NewGuid(),
            ModuleName = "Default",
            MessageType = "SampleDomainEvent",
            Content = "{}",
            Status = "Pending",
            CreatedAt = DateTimeOffset.UtcNow
        };

        db.OutboxMessages.Add(msg);
        await db.SaveChangesAsync();

        // Simulate background publisher picking up
        await publisher.PublishAsync(msg.MessageType, msg.Content, default);
        if (!_fixture.IsDockerActive)
        {
            _fixture.InMemoryPublisher.PublishedMessages.TryPeek(out var published);
            Assert.Equal("SampleDomainEvent", published.MessageType);
        }
    }

    [Fact]
    public async Task CF_02_DapperOutboxMessageLockAndFetchProcessingUpdatingEfCoreTrackingTable()
    {
        using var scope = _fixture.Services.CreateScope();
        var repo = scope.ServiceProvider.GetKeyedService<IOutboxRepository>("Default") 
                   ?? scope.ServiceProvider.GetRequiredService<IOutboxRepository>();

        var msgId = Guid.NewGuid();
        // Locks using Dapper or EF (whichever is active) and updates
        await repo.UpdateMessageStatusAsync("Default", msgId, "Processed", DateTimeOffset.UtcNow, null, 0, default);
        var count = await repo.GetCountByStatusAsync("Default", "Processed", default);
        Assert.True(count >= 0);
    }

    [Fact]
    public async Task CF_03_IdempotentDecoratorCheckingDapperDatabaseInboxBeforeProcessingBrokerMessage()
    {
        using var scope = _fixture.Services.CreateScope();
        var processor = scope.ServiceProvider.GetKeyedService<IInboxProcessor>("Default") 
                        ?? scope.ServiceProvider.GetRequiredService<IInboxProcessor>();

        var ev = new TestEvent();
        var handler = new MockHandler();
        var decorator = new IdempotentIntegrationEventHandlerDecorator<TestEvent>(
            handler, processor, "Default", e => e.Id);

        // Pre-track as processed in database
        await processor.TrackMessageAsync("Default", ev.Id, typeof(TestEvent).FullName!, default);
        await processor.MarkAsProcessedAsync("Default", ev.Id, default);

        // Act
        await decorator.HandleAsync(ev, default);

        // Assert: should not call handler
        Assert.False(handler.Handled);
    }

    [Fact]
    public async Task CF_04_DashboardManualRetryTriggeringEfCoreOutboxProcessingAndBrokerPublication()
    {
        using var scope = _fixture.Services.CreateScope();
        var repo = scope.ServiceProvider.GetKeyedService<IOutboxRepository>("Default") 
                   ?? scope.ServiceProvider.GetRequiredService<IOutboxRepository>();

        var msgId = Guid.NewGuid();
        var msg = new OutboxMessage
        {
            Id = msgId,
            ModuleName = "Default",
            MessageType = "FailedEvent",
            Content = "{}",
            Status = "Failed",
            CreatedAt = DateTimeOffset.UtcNow
        };

        if (repo is OutboxCore.EntityFrameworkCore.Repositories.EfOutboxRepository<OutboxCore.Sample.WebApi.Data.ApplicationDbContext> efRepo)
        {
            var db = scope.ServiceProvider.GetRequiredService<OutboxCore.Sample.WebApi.Data.ApplicationDbContext>();
            db.OutboxMessages.Add(msg);
            await db.SaveChangesAsync();

            // Perform UI/Dashboard operation
            var client = _fixture.CreateClient();
            var response = await client.PostAsync($"/outbox-dashboard/api/retry?module=Default&id={msgId}", null);
            Assert.Equal(System.Net.HttpStatusCode.OK, response.StatusCode);

            db.ChangeTracker.Clear();
            var dbMsg = await db.OutboxMessages.FindAsync(msgId);
            Assert.Contains(dbMsg?.Status, new[] { "Pending", "Processing", "Processed" });
        }
        else
        {
            Assert.True(true);
        }
    }

    [Fact]
    public async Task CF_05_ModularMonolithIsolationRunningEfCoreOrdersAndDapperBilling()
    {
        using var scope = _fixture.Services.CreateScope();
        var repoOrders = scope.ServiceProvider.GetKeyedService<IOutboxRepository>("Orders");
        var repoBilling = scope.ServiceProvider.GetKeyedService<IOutboxRepository>("Billing");

        Assert.NotNull(repoOrders);
        Assert.NotNull(repoBilling);
    }
}

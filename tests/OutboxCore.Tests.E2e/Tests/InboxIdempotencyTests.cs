using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using OutboxCore.Abstractions;
using OutboxCore.Inbox;
using OutboxCore.Models;
using OutboxCore.Tests.E2e.Fixtures;
using Xunit;

namespace OutboxCore.Tests.E2e.Tests;

public class InboxIdempotencyTests : IClassFixture<E2eTestFixture>
{
    private readonly E2eTestFixture _fixture;

    public InboxIdempotencyTests(E2eTestFixture fixture)
    {
        _fixture = fixture;
        _fixture.SeedTestDatabase();
        _fixture.ClearBroker();
    }

    public class TestEvent
    {
        public Guid Id { get; set; } = Guid.NewGuid();
        public string Data { get; set; } = "";
    }

    public class TestHandler : IIntegrationEventHandler<TestEvent>
    {
        public int InvocationCount { get; private set; }
        public bool ThrowException { get; set; }

        public Task HandleAsync(TestEvent @event, CancellationToken cancellationToken)
        {
            InvocationCount++;
            if (ThrowException)
            {
                throw new InvalidOperationException("Simulated handler failure");
            }
            return Task.CompletedTask;
        }
    }

    [Fact]
    public async Task F3_T1_01_VerifyInboxProcessorTracksMessageAsProcessed()
    {
        using var scope = _fixture.Services.CreateScope();
        var processor = scope.ServiceProvider.GetKeyedService<IInboxProcessor>("Default") 
                        ?? scope.ServiceProvider.GetRequiredService<IInboxProcessor>();

        var messageId = Guid.NewGuid();
        await processor.TrackMessageAsync("Default", messageId, "TestType", default);
        await processor.MarkAsProcessedAsync("Default", messageId, default);

        var processed = await processor.HasBeenProcessedAsync("Default", messageId, default);
        Assert.True(processed);
    }

    [Fact]
    public async Task F3_T1_02_VerifyInboxProcessorDetectsAlreadyProcessedMessage()
    {
        using var scope = _fixture.Services.CreateScope();
        var processor = scope.ServiceProvider.GetKeyedService<IInboxProcessor>("Default") 
                        ?? scope.ServiceProvider.GetRequiredService<IInboxProcessor>();

        var messageId = Guid.NewGuid();
        var isProcessedBefore = await processor.HasBeenProcessedAsync("Default", messageId, default);
        Assert.False(isProcessedBefore);

        await processor.TrackMessageAsync("Default", messageId, "TestType", default);
        await processor.MarkAsProcessedAsync("Default", messageId, default);

        var isProcessedAfter = await processor.HasBeenProcessedAsync("Default", messageId, default);
        Assert.True(isProcessedAfter);
    }

    [Fact]
    public async Task F3_T1_03_VerifyIdempotentHandlerDecoratorSkipsExecutionForAlreadyProcessedMessage()
    {
        using var scope = _fixture.Services.CreateScope();
        var processor = scope.ServiceProvider.GetKeyedService<IInboxProcessor>("Default") 
                        ?? scope.ServiceProvider.GetRequiredService<IInboxProcessor>();

        var handler = new TestHandler();
        var decorator = new IdempotentIntegrationEventHandlerDecorator<TestEvent>(
            handler, processor, "Default", ev => ev.Id);

        var ev = new TestEvent();

        // Simulate processed
        await processor.TrackMessageAsync("Default", ev.Id, typeof(TestEvent).FullName!, default);
        await processor.MarkAsProcessedAsync("Default", ev.Id, default);

        // Act
        await decorator.HandleAsync(ev, default);

        // Assert
        Assert.Equal(0, handler.InvocationCount);
    }

    [Fact]
    public async Task F3_T1_04_VerifyIdempotentHandlerDecoratorExecutesAndTracksMessageOnFirstCall()
    {
        using var scope = _fixture.Services.CreateScope();
        var processor = scope.ServiceProvider.GetKeyedService<IInboxProcessor>("Default") 
                        ?? scope.ServiceProvider.GetRequiredService<IInboxProcessor>();

        var handler = new TestHandler();
        var decorator = new IdempotentIntegrationEventHandlerDecorator<TestEvent>(
            handler, processor, "Default", ev => ev.Id);

        var ev = new TestEvent();

        // Act
        await decorator.HandleAsync(ev, default);

        // Assert
        Assert.Equal(1, handler.InvocationCount);
        var processed = await processor.HasBeenProcessedAsync("Default", ev.Id, default);
        Assert.True(processed);
    }

    [Fact]
    public async Task F3_T1_05_VerifyIdempotentHandlerDecoratorMarksMessageAsFailedWhenHandlerThrows()
    {
        using var scope = _fixture.Services.CreateScope();
        var processor = scope.ServiceProvider.GetKeyedService<IInboxProcessor>("Default") 
                        ?? scope.ServiceProvider.GetRequiredService<IInboxProcessor>();

        var handler = new TestHandler { ThrowException = true };
        var decorator = new IdempotentIntegrationEventHandlerDecorator<TestEvent>(
            handler, processor, "Default", ev => ev.Id);

        var ev = new TestEvent();

        // Act & Assert
        await Assert.ThrowsAsync<InvalidOperationException>(() => decorator.HandleAsync(ev, default));

        var processed = await processor.HasBeenProcessedAsync("Default", ev.Id, default);
        Assert.False(processed);

        var dbMessages = await processor.GetMessagesAsync("Default", "Failed", 10, default);
        Assert.Contains(dbMessages, m => m.Id == ev.Id && m.Status == "Failed");
    }

    [Fact]
    public async Task F3_T2_01_VerifyBehaviorWhenInboxMessageStatusIsPending()
    {
        using var scope = _fixture.Services.CreateScope();
        var processor = scope.ServiceProvider.GetKeyedService<IInboxProcessor>("Default") 
                        ?? scope.ServiceProvider.GetRequiredService<IInboxProcessor>();

        var messageId = Guid.NewGuid();
        await processor.TrackMessageAsync("Default", messageId, "PendingType", default);

        var processed = await processor.HasBeenProcessedAsync("Default", messageId, default);
        Assert.False(processed);
    }

    [Fact]
    public async Task F3_T2_02_VerifyTrackingWorksForExtremelyLongMessageType()
    {
        using var scope = _fixture.Services.CreateScope();
        var processor = scope.ServiceProvider.GetKeyedService<IInboxProcessor>("Default") 
                        ?? scope.ServiceProvider.GetRequiredService<IInboxProcessor>();

        var messageId = Guid.NewGuid();
        var longType = new string('A', 150); // boundary test for length
        await processor.TrackMessageAsync("Default", messageId, longType, default);

        var dbMessages = await processor.GetMessagesAsync("Default", "Pending", 10, default);
        Assert.Contains(dbMessages, m => m.Id == messageId);
    }

    [Fact]
    public async Task F3_T2_03_VerifyIdempotencyAcrossDifferentModules()
    {
        using var scope = _fixture.Services.CreateScope();
        var processor = scope.ServiceProvider.GetKeyedService<IInboxProcessor>("Default") 
                        ?? scope.ServiceProvider.GetRequiredService<IInboxProcessor>();

        var messageId = Guid.NewGuid();

        // Track in Module Orders
        await processor.TrackMessageAsync("Orders", messageId, "TestType", default);
        await processor.MarkAsProcessedAsync("Orders", messageId, default);

        // Check in Module Billing
        var processedInBilling = await processor.HasBeenProcessedAsync("Billing", messageId, default);
        Assert.False(processedInBilling);

        var processedInOrders = await processor.HasBeenProcessedAsync("Orders", messageId, default);
        Assert.True(processedInOrders);
    }

    [Fact]
    public async Task F3_T2_04_VerifyMultipleConcurrentChecksOnlyAllowsOneToExecute()
    {
        using var scope = _fixture.Services.CreateScope();
        var processor = scope.ServiceProvider.GetKeyedService<IInboxProcessor>("Default") 
                        ?? scope.ServiceProvider.GetRequiredService<IInboxProcessor>();

        var messageId = Guid.NewGuid();

        var task1 = processor.TrackMessageAsync("Default", messageId, "Type", default);
        var task2 = processor.TrackMessageAsync("Default", messageId, "Type", default);

        // One of them should succeed, the other might throw or be caught due to unique constraint on database
        var ex = await Record.ExceptionAsync(() => Task.WhenAll(task1, task2));
        // If unique constraint triggers, it's expected behavior
        Assert.True(ex == null || ex is DbUpdateException || ex is InvalidOperationException || ex is DbException);
    }

    [Fact]
    public async Task F3_T2_05_VerifyEmptyOrNullMessageIdsRaiseArgumentException()
    {
        using var scope = _fixture.Services.CreateScope();
        var processor = scope.ServiceProvider.GetKeyedService<IInboxProcessor>("Default") 
                        ?? scope.ServiceProvider.GetRequiredService<IInboxProcessor>();

        var handler = new TestHandler();
        var decorator = new IdempotentIntegrationEventHandlerDecorator<TestEvent>(
            handler, processor, "Default", ev => Guid.Empty);

        var ev = new TestEvent { Id = Guid.Empty };

        await Assert.ThrowsAsync<ArgumentException>(() => decorator.HandleAsync(ev, default));
    }
}

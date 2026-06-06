using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Diagnostics;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Moq;
using OutboxCore.Abstractions;
using OutboxCore.Background;
using OutboxCore.Configuration;
using OutboxCore.Dialects;
using OutboxCore.EntityFrameworkCore.Extensions;
using OutboxCore.EntityFrameworkCore.Interceptors;
using OutboxCore.Models;
using Xunit;
using Xunit.Abstractions;

namespace OutboxCore.Tests.Unit;

public class ChallengeEvent : IDomainEvent
{
    public Guid Id { get; } = Guid.NewGuid();
    public DateTimeOffset OccurredOn { get; } = DateTimeOffset.UtcNow;
    public string Data { get; set; } = string.Empty;
}

public class ChallengeEntity : AggregateRoot
{
    public Guid Id { get; set; }
    public string Name { get; set; } = string.Empty;

    public void DoSomething()
    {
        RaiseDomainEvent(new ChallengeEvent { Data = Name });
    }
}

public class ChallengeDbContext : DbContext
{
    public DbSet<ChallengeEntity> ChallengeEntities { get; set; } = null!;
    public DbSet<OutboxMessage> OutboxMessages { get; set; } = null!;
    public DbSet<InboxMessage> InboxMessages { get; set; } = null!;

    public ChallengeDbContext(DbContextOptions<ChallengeDbContext> options) : base(options)
    {
    }

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        base.OnModelCreating(modelBuilder);
        modelBuilder.ApplyOutboxConfigurations();

        modelBuilder.Entity<ChallengeEntity>(builder =>
        {
            builder.HasKey(x => x.Id);
        });
    }
}

public class Milestone1ChallengeTests
{
    private readonly ITestOutputHelper _output;

    public Milestone1ChallengeTests(ITestOutputHelper output)
    {
        _output = output;
    }

    [Fact]
    public async Task Challenge_BackgroundService_ParallelModules()
    {
        _output.WriteLine("Starting Challenge_BackgroundService_ParallelModules");
        
        var services = new ServiceCollection();
        
        var options = new OutboxOptions();
        options.RegisterModule("ModuleA", opts => {
            opts.BatchSize = 10;
            opts.PollingInterval = TimeSpan.FromMilliseconds(50);
            opts.LockDuration = TimeSpan.FromSeconds(5);
        });
        options.RegisterModule("ModuleB", opts => {
            opts.BatchSize = 10;
            opts.PollingInterval = TimeSpan.FromMilliseconds(50);
            opts.LockDuration = TimeSpan.FromSeconds(5);
        });

        var optionsMock = new Mock<IOptions<OutboxOptions>>();
        optionsMock.Setup(o => o.Value).Returns(options);

        var channelMock = new Mock<IOutboxChannel>();
        channelMock.Setup(c => c.WaitToReadAsync(It.IsAny<string>(), It.IsAny<CancellationToken>()))
            .Returns(async (string _, CancellationToken ct) =>
            {
                try
                {
                    await Task.Delay(Timeout.Infinite, ct);
                }
                catch (OperationCanceledException)
                {
                }
                return false;
            });

        var repoAMock = new Mock<IOutboxRepository>();
        var repoBMock = new Mock<IOutboxRepository>();
        var pubAMock = new Mock<IMessagePublisher>();
        var pubBMock = new Mock<IMessagePublisher>();

        var msgA = new OutboxMessage { Id = Guid.NewGuid(), ModuleName = "ModuleA", MessageType = "EventA", Content = "ContentA", Status = "Pending" };
        var msgB = new OutboxMessage { Id = Guid.NewGuid(), ModuleName = "ModuleB", MessageType = "EventB", Content = "ContentB", Status = "Pending" };

        var msgAQueue = new Queue<List<OutboxMessage>>();
        msgAQueue.Enqueue(new List<OutboxMessage> { msgA });
        msgAQueue.Enqueue(new List<OutboxMessage>());

        var msgBQueue = new Queue<List<OutboxMessage>>();
        msgBQueue.Enqueue(new List<OutboxMessage> { msgB });
        msgBQueue.Enqueue(new List<OutboxMessage>());

        repoAMock.Setup(r => r.LockMessagesAsync("ModuleA", It.IsAny<string>(), It.IsAny<TimeSpan>(), It.IsAny<int>(), It.IsAny<CancellationToken>()))
            .Callback<string, string, TimeSpan, int, CancellationToken>((mod, worker, dur, batch, ct) => {
                _output.WriteLine($"[RepoA] LockMessagesAsync called. Queue Count: {msgAQueue.Count}");
            })
            .ReturnsAsync(() => msgAQueue.Count > 0 ? msgAQueue.Dequeue() : new List<OutboxMessage>());

        repoBMock.Setup(r => r.LockMessagesAsync("ModuleB", It.IsAny<string>(), It.IsAny<TimeSpan>(), It.IsAny<int>(), It.IsAny<CancellationToken>()))
            .Callback<string, string, TimeSpan, int, CancellationToken>((mod, worker, dur, batch, ct) => {
                _output.WriteLine($"[RepoB] LockMessagesAsync called. Queue Count: {msgBQueue.Count}");
            })
            .ReturnsAsync(() => msgBQueue.Count > 0 ? msgBQueue.Dequeue() : new List<OutboxMessage>());

        var pubAExecuted = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        var pubBExecuted = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);

        pubAMock.Setup(p => p.PublishAsync("EventA", "ContentA", It.IsAny<CancellationToken>()))
            .Callback<string, string, CancellationToken>((type, content, ct) => {
                _output.WriteLine("[PubA] PublishAsync called!");
                pubAExecuted.TrySetResult(true);
            })
            .Returns(Task.CompletedTask);

        pubBMock.Setup(p => p.PublishAsync("EventB", "ContentB", It.IsAny<CancellationToken>()))
            .Callback<string, string, CancellationToken>((type, content, ct) => {
                _output.WriteLine("[PubB] PublishAsync called!");
                pubBExecuted.TrySetResult(true);
            })
            .Returns(Task.CompletedTask);

        services.AddKeyedSingleton<IOutboxRepository>("ModuleA", repoAMock.Object);
        services.AddKeyedSingleton<IOutboxRepository>("ModuleB", repoBMock.Object);
        services.AddKeyedSingleton<IMessagePublisher>("ModuleA", pubAMock.Object);
        services.AddKeyedSingleton<IMessagePublisher>("ModuleB", pubBMock.Object);

        var serviceProvider = services.BuildServiceProvider();
        var loggerMock = new Mock<ILogger<OutboxPublisherBackgroundService>>();
        
        loggerMock.Setup(x => x.Log(
            It.IsAny<LogLevel>(),
            It.IsAny<EventId>(),
            It.IsAny<It.IsAnyType>(),
            It.IsAny<Exception>(),
            It.IsAny<Func<It.IsAnyType, Exception?, string>>()))
            .Callback(new InvocationAction(invocation =>
            {
                var logLevel = invocation.Arguments[0];
                var state = invocation.Arguments[2];
                var exception = invocation.Arguments[3] as Exception;
                var formatter = invocation.Arguments[4];
                var message = formatter.GetType().GetMethod("Invoke").Invoke(formatter, new[] { state, exception });
                _output.WriteLine($"[BG SERVICE] [{logLevel}] {message}");
            }));

        var backgroundService = new OutboxPublisherBackgroundService(serviceProvider, channelMock.Object, optionsMock.Object, loggerMock.Object);

        using var cts = new CancellationTokenSource();
        var runTask = backgroundService.StartAsync(cts.Token);

        _output.WriteLine("Background service started, waiting for completion sources...");
        var delayTask = Task.Delay(5000);
        var completedTask = await Task.WhenAny(Task.WhenAll(pubAExecuted.Task, pubBExecuted.Task), delayTask);

        _output.WriteLine("Cancelling background service...");
        cts.Cancel();
        try { await runTask; } catch (OperationCanceledException) { }

        if (completedTask == delayTask)
        {
            _output.WriteLine("TIMEOUT! Background service failed to process both messages within 5 seconds.");
            throw new Exception("Timeout waiting for background service tasks to execute.");
        }

        _output.WriteLine("Success! Both modules executed independently.");
        pubAMock.Verify(p => p.PublishAsync("EventA", "ContentA", It.IsAny<CancellationToken>()), Times.Once);
        pubBMock.Verify(p => p.PublishAsync("EventB", "ContentB", It.IsAny<CancellationToken>()), Times.Once);
    }

    [Fact]
    public async Task Challenge_ScopedInterceptor_Isolation()
    {
        _output.WriteLine("Starting Challenge_ScopedInterceptor_Isolation");

        using var connection = new SqliteConnection("Filename=:memory:");
        await connection.OpenAsync();

        var channelMock = new Mock<IOutboxChannel>();
        channelMock.Setup(c => c.WriteAsync(It.IsAny<string>(), It.IsAny<CancellationToken>()))
            .Callback<string, CancellationToken>((module, ct) => {
                _output.WriteLine($"[Channel] WriteAsync called for: {module}");
            })
            .Returns(ValueTask.CompletedTask);

        var moduleSelectorMock = new Mock<IModuleSelector>();
        moduleSelectorMock.Setup(m => m.GetModuleName(It.Is<object>(x => ((ChallengeEntity)x).Name == "EntityA"), It.IsAny<IDomainEvent>()))
            .Returns("ModuleA");
        moduleSelectorMock.Setup(m => m.GetModuleName(It.Is<object>(x => ((ChallengeEntity)x).Name == "EntityB"), It.IsAny<IDomainEvent>()))
            .Returns("ModuleB");

        var options = new DbContextOptionsBuilder<ChallengeDbContext>()
            .UseSqlite(connection)
            .Options;

        using (var context = new ChallengeDbContext(options))
        {
            await context.Database.EnsureCreatedAsync();
        }

        using var contextA = new ChallengeDbContext(options);
        using var contextB = new ChallengeDbContext(options);

        var entityA = new ChallengeEntity { Id = Guid.NewGuid(), Name = "EntityA" };
        entityA.DoSomething();
        contextA.ChallengeEntities.Add(entityA);

        var entityB = new ChallengeEntity { Id = Guid.NewGuid(), Name = "EntityB" };
        entityB.DoSomething();
        contextB.ChallengeEntities.Add(entityB);

        _output.WriteLine("Entities added. Setting up interceptors...");
        var scopedInterceptorA = new OutboxSaveChangesInterceptor(channelMock.Object, moduleSelectorMock.Object);
        var scopedInterceptorB = new OutboxSaveChangesInterceptor(channelMock.Object, moduleSelectorMock.Object);

        _output.WriteLine("Invoking SavingChanges on scopedInterceptorA...");
        scopedInterceptorA.SavingChanges(
            new DbContextEventData((EventDefinitionBase)null!, (d, s) => "", contextA), 
            new InterceptionResult<int>()
        );

        _output.WriteLine("Invoking SavingChanges on scopedInterceptorB...");
        scopedInterceptorB.SavingChanges(
            new DbContextEventData((EventDefinitionBase)null!, (d, s) => "", contextB), 
            new InterceptionResult<int>()
        );

        _output.WriteLine("Invoking SavedChanges on scopedInterceptorA...");
        scopedInterceptorA.SavedChanges(
            new SaveChangesCompletedEventData((EventDefinitionBase)null!, (d, s) => "", contextA, 1), 
            1
        );

        _output.WriteLine("Verifying scopedInterceptorA notifications...");
        channelMock.Verify(c => c.WriteAsync("ModuleA", It.IsAny<CancellationToken>()), Times.Once);
        channelMock.Verify(c => c.WriteAsync("ModuleB", It.IsAny<CancellationToken>()), Times.Never);

        channelMock.Invocations.Clear();

        _output.WriteLine("Invoking SavedChanges on scopedInterceptorB...");
        scopedInterceptorB.SavedChanges(
            new SaveChangesCompletedEventData((EventDefinitionBase)null!, (d, s) => "", contextB, 1), 
            1
        );

        _output.WriteLine("Verifying scopedInterceptorB notifications...");
        channelMock.Verify(c => c.WriteAsync("ModuleB", It.IsAny<CancellationToken>()), Times.Once);
        channelMock.Verify(c => c.WriteAsync("ModuleA", It.IsAny<CancellationToken>()), Times.Never);
    }
}

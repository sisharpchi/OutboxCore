using System;
using System.Linq;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Diagnostics;
using OutboxCore.Abstractions;
using OutboxCore.Background;
using OutboxCore.Models;

namespace OutboxCore.EntityFrameworkCore.Interceptors;

public class OutboxSaveChangesInterceptor : SaveChangesInterceptor
{
    private readonly IOutboxChannel _channel;
    private bool _hasOutboxMessages;

    public OutboxSaveChangesInterceptor(IOutboxChannel channel)
    {
        _channel = channel;
    }

    public override InterceptionResult<int> SavingChanges(DbContextEventData eventData, InterceptionResult<int> result)
    {
        ConvertDomainEventsToOutboxMessages(eventData.Context);
        return base.SavingChanges(eventData, result);
    }

    public override ValueTask<InterceptionResult<int>> SavingChangesAsync(
        DbContextEventData eventData,
        InterceptionResult<int> result,
        CancellationToken cancellationToken = default)
    {
        ConvertDomainEventsToOutboxMessages(eventData.Context);
        return base.SavingChangesAsync(eventData, result, cancellationToken);
    }

    public override int SavedChanges(SaveChangesCompletedEventData eventData, int result)
    {
        NotifyOutboxChannel();
        return base.SavedChanges(eventData, result);
    }

    public override async ValueTask<int> SavedChangesAsync(
        SaveChangesCompletedEventData eventData,
        int result,
        CancellationToken cancellationToken = default)
    {
        await NotifyOutboxChannelAsync(cancellationToken);
        return await base.SavedChangesAsync(eventData, result, cancellationToken);
    }

    private void ConvertDomainEventsToOutboxMessages(DbContext? context)
    {
        if (context == null) return;

        var entities = context.ChangeTracker
            .Entries<IHasDomainEvents>()
            .Where(x => x.Entity.DomainEvents != null && x.Entity.DomainEvents.Any())
            .ToList();

        var domainEvents = entities
            .SelectMany(x => x.Entity.DomainEvents)
            .ToList();

        if (!domainEvents.Any())
        {
            _hasOutboxMessages = false;
            return;
        }

        _hasOutboxMessages = true;

        var outboxMessages = domainEvents.Select(domainEvent => new OutboxMessage
        {
            Id = domainEvent.Id == Guid.Empty ? Guid.NewGuid() : domainEvent.Id,
            MessageType = domainEvent.GetType().FullName ?? domainEvent.GetType().Name,
            Content = JsonSerializer.Serialize(domainEvent, domainEvent.GetType()),
            CreatedAt = domainEvent.OccurredOn == default ? DateTimeOffset.UtcNow : domainEvent.OccurredOn,
            Status = "Pending",
            RetryCount = 0
        }).ToList();

        context.Set<OutboxMessage>().AddRange(outboxMessages);

        foreach (var entityEntry in entities)
        {
            entityEntry.Entity.ClearDomainEvents();
        }
    }

    private void NotifyOutboxChannel()
    {
        if (_hasOutboxMessages)
        {
            _channel.WriteAsync().GetAwaiter().GetResult();
            _hasOutboxMessages = false;
        }
    }

    private async ValueTask NotifyOutboxChannelAsync(CancellationToken cancellationToken)
    {
        if (_hasOutboxMessages)
        {
            await _channel.WriteAsync(cancellationToken);
            _hasOutboxMessages = false;
        }
    }
}

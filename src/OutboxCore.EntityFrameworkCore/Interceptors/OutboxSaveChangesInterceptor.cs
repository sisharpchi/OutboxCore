using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
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
    private readonly string? _moduleName;
    private readonly IModuleSelector? _moduleSelector;
    private readonly ConcurrentBag<string> _modulesToNotify = new();

    public OutboxSaveChangesInterceptor(IOutboxChannel channel)
    {
        _channel = channel;
    }

    public OutboxSaveChangesInterceptor(IOutboxChannel channel, string moduleName)
    {
        _channel = channel;
        _moduleName = moduleName;
    }

    public OutboxSaveChangesInterceptor(IOutboxChannel channel, IModuleSelector moduleSelector)
    {
        _channel = channel;
        _moduleSelector = moduleSelector;
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

        var entries = context.ChangeTracker
            .Entries<IHasDomainEvents>()
            .Where(x => x.Entity.DomainEvents != null && x.Entity.DomainEvents.Any())
            .ToList();

        if (!entries.Any()) return;

        var outboxMessages = new List<OutboxMessage>();

        foreach (var entry in entries)
        {
            foreach (var domainEvent in entry.Entity.DomainEvents)
            {
                var module = _moduleName ?? _moduleSelector?.GetModuleName(entry.Entity, domainEvent) ?? "Default";
                
                _modulesToNotify.Add(module);

                outboxMessages.Add(new OutboxMessage
                {
                    Id = domainEvent.Id == Guid.Empty ? Guid.NewGuid() : domainEvent.Id,
                    ModuleName = module,
                    MessageType = domainEvent.GetType().FullName ?? domainEvent.GetType().Name,
                    Content = JsonSerializer.Serialize(domainEvent, domainEvent.GetType()),
                    CreatedAt = domainEvent.OccurredOn == default ? DateTimeOffset.UtcNow : domainEvent.OccurredOn,
                    Status = "Pending",
                    RetryCount = 0
                });
            }
            entry.Entity.ClearDomainEvents();
        }

        context.Set<OutboxMessage>().AddRange(outboxMessages);
    }

    private void NotifyOutboxChannel()
    {
        while (_modulesToNotify.TryTake(out var moduleName))
        {
            _channel.WriteAsync(moduleName).GetAwaiter().GetResult();
        }
    }

    private async ValueTask NotifyOutboxChannelAsync(CancellationToken cancellationToken)
    {
        while (_modulesToNotify.TryTake(out var moduleName))
        {
            await _channel.WriteAsync(moduleName, cancellationToken);
        }
    }
}

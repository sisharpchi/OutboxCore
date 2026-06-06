using System;
using System.Threading;
using System.Threading.Tasks;
using OutboxCore.Abstractions;

namespace OutboxCore.Inbox;

public interface IIntegrationEventHandler<TEvent>
{
    Task HandleAsync(TEvent @event, CancellationToken cancellationToken);
}

public class IdempotentIntegrationEventHandlerDecorator<TEvent> : IIntegrationEventHandler<TEvent>
{
    private readonly IIntegrationEventHandler<TEvent> _innerHandler;
    private readonly IInboxProcessor _inboxProcessor;
    private readonly string _moduleName;
    private readonly Func<TEvent, Guid> _eventIdSelector;

    public IdempotentIntegrationEventHandlerDecorator(
        IIntegrationEventHandler<TEvent> innerHandler,
        IInboxProcessor inboxProcessor,
        string moduleName,
        Func<TEvent, Guid> eventIdSelector)
    {
        _innerHandler = innerHandler;
        _inboxProcessor = inboxProcessor;
        _moduleName = moduleName;
        _eventIdSelector = eventIdSelector;
    }

    public async Task HandleAsync(TEvent @event, CancellationToken cancellationToken)
    {
        if (@event == null) throw new ArgumentNullException(nameof(@event));
        
        var messageId = _eventIdSelector(@event);
        if (messageId == Guid.Empty)
        {
            throw new ArgumentException("Message ID cannot be empty Guid.", nameof(@event));
        }

        var messageType = typeof(TEvent).FullName ?? typeof(TEvent).Name;

        if (await _inboxProcessor.HasBeenProcessedAsync(_moduleName, messageId, cancellationToken))
        {
            return;
        }

        try
        {
            await _inboxProcessor.TrackMessageAsync(_moduleName, messageId, messageType, cancellationToken);
        }
        catch
        {
            if (await _inboxProcessor.HasBeenProcessedAsync(_moduleName, messageId, cancellationToken))
            {
                return;
            }
            throw;
        }

        try
        {
            await _innerHandler.HandleAsync(@event, cancellationToken);
            await _inboxProcessor.MarkAsProcessedAsync(_moduleName, messageId, cancellationToken);
        }
        catch (Exception ex)
        {
            await _inboxProcessor.MarkAsFailedAsync(_moduleName, messageId, ex.Message, cancellationToken);
            throw;
        }
    }
}

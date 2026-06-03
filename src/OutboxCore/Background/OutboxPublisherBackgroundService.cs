using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using OutboxCore.Abstractions;
using OutboxCore.Configuration;

namespace OutboxCore.Background;

public class OutboxPublisherBackgroundService : BackgroundService
{
    private readonly IServiceProvider _serviceProvider;
    private readonly IOutboxChannel _channel;
    private readonly OutboxOptions _options;
    private readonly ILogger<OutboxPublisherBackgroundService> _logger;
    private readonly string _workerId;

    public OutboxPublisherBackgroundService(
        IServiceProvider serviceProvider,
        IOutboxChannel channel,
        IOptions<OutboxOptions> options,
        ILogger<OutboxPublisherBackgroundService> logger)
    {
        _serviceProvider = serviceProvider;
        _channel = channel;
        _options = options.Value;
        _logger = logger;
        _workerId = $"Worker-{Guid.NewGuid()}-{Environment.MachineName}";
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _logger.LogInformation("OutboxPublisherBackgroundService started with Worker ID {WorkerId}", _workerId);

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                bool processedAny = await ProcessOutboxMessagesAsync(stoppingToken);

                if (!processedAny)
                {
                    // Wait for either a notification via the Channel or the PollingInterval timeout
                    using var cts = CancellationTokenSource.CreateLinkedTokenSource(stoppingToken);
                    cts.CancelAfter(_options.PollingInterval);

                    try
                    {
                        await _channel.WaitToReadAsync(cts.Token);
                    }
                    catch (OperationCanceledException) when (!stoppingToken.IsCancellationRequested)
                    {
                        // Polling timeout reached, continue to poll
                    }
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error occurred while processing outbox messages");
                try
                {
                    await Task.Delay(_options.PollingInterval, stoppingToken);
                }
                catch (OperationCanceledException)
                {
                    // Service is stopping
                }
            }
        }
    }

    private async Task<bool> ProcessOutboxMessagesAsync(CancellationToken cancellationToken)
    {
        using var scope = _serviceProvider.CreateScope();
        var repository = scope.ServiceProvider.GetService<IOutboxRepository>();
        var publisher = scope.ServiceProvider.GetService<IMessagePublisher>();

        if (repository == null || publisher == null)
        {
            _logger.LogWarning("IOutboxRepository or IMessagePublisher is not registered. Skipping outbox processing.");
            return false;
        }

        var messages = await repository.LockMessagesAsync(_workerId, _options.LockDuration, _options.BatchSize, cancellationToken);
        if (messages.Count == 0)
        {
            return false;
        }

        _logger.LogDebug("Worker {WorkerId} locked {Count} outbox messages for processing", _workerId, messages.Count);

        foreach (var message in messages)
        {
            try
            {
                await publisher.PublishAsync(message.MessageType, message.Content, cancellationToken);

                if (_options.DeleteOnPublish)
                {
                    await repository.DeleteMessageAsync(message.Id, cancellationToken);
                }
                else
                {
                    await repository.UpdateMessageStatusAsync(
                        message.Id,
                        "Processed",
                        DateTimeOffset.UtcNow,
                        null,
                        message.RetryCount,
                        cancellationToken);
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Failed to publish outbox message {MessageId} (Type: {MessageType})", message.Id, message.MessageType);

                int nextRetryCount = message.RetryCount + 1;
                string status = nextRetryCount >= _options.MaxRetryCount ? "Failed" : "Pending";

                await repository.UpdateMessageStatusAsync(
                    message.Id,
                    status,
                    null,
                    ex.ToString(),
                    nextRetryCount,
                    cancellationToken);
            }
        }

        return true;
    }
}

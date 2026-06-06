using System;
using System.Collections.Generic;
using System.Linq;
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

        var modules = _options.Modules;
        if (modules.Count == 0)
        {
            modules = new List<OutboxModuleOptions>
            {
                new OutboxModuleOptions
                {
                    ModuleName = "Default",
                    BatchSize = _options.BatchSize,
                    PollingInterval = _options.PollingInterval,
                    LockDuration = _options.LockDuration,
                    MaxRetryCount = _options.MaxRetryCount,
                    DeleteOnPublish = _options.DeleteOnPublish
                }
            };
        }

        var tasks = modules.Select(module => RunModuleLoopAsync(module, stoppingToken)).ToList();
        await Task.WhenAll(tasks);
    }

    private async Task RunModuleLoopAsync(OutboxModuleOptions moduleOptions, CancellationToken stoppingToken)
    {
        var moduleName = moduleOptions.ModuleName;
        _logger.LogInformation("Starting outbox processing loop for module {ModuleName}", moduleName);

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                bool processedAny = await ProcessOutboxMessagesAsync(moduleName, moduleOptions, stoppingToken);

                if (!processedAny)
                {
                    using var cts = CancellationTokenSource.CreateLinkedTokenSource(stoppingToken);
                    cts.CancelAfter(moduleOptions.PollingInterval);

                    try
                    {
                        await _channel.WaitToReadAsync(moduleName, cts.Token);
                    }
                    catch (OperationCanceledException) when (!stoppingToken.IsCancellationRequested)
                    {
                        // Polling interval elapsed, continue to lock and process
                    }
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error occurred while processing outbox messages for module {ModuleName}", moduleName);
                try
                {
                    await Task.Delay(moduleOptions.PollingInterval, stoppingToken);
                }
                catch (OperationCanceledException)
                {
                    // Service stopping
                }
            }
        }
    }

    private async Task<bool> ProcessOutboxMessagesAsync(
        string moduleName,
        OutboxModuleOptions moduleOptions,
        CancellationToken cancellationToken)
    {
        using var scope = _serviceProvider.CreateScope();
        
        var repository = scope.ServiceProvider.GetKeyedService<IOutboxRepository>(moduleName) 
                         ?? scope.ServiceProvider.GetService<IOutboxRepository>();
        var publisher = scope.ServiceProvider.GetKeyedService<IMessagePublisher>(moduleName)
                        ?? scope.ServiceProvider.GetService<IMessagePublisher>();

        if (repository == null || publisher == null)
        {
            _logger.LogWarning("IOutboxRepository or IMessagePublisher is not registered for module {ModuleName}. Skipping outbox processing.", moduleName);
            return false;
        }

        var messages = await repository.LockMessagesAsync(moduleName, _workerId, moduleOptions.LockDuration, moduleOptions.BatchSize, cancellationToken);
        if (messages.Count == 0)
        {
            return false;
        }

        _logger.LogDebug("Worker {WorkerId} locked {Count} outbox messages for module {ModuleName}", _workerId, messages.Count, moduleName);

        foreach (var message in messages)
        {
            try
            {
                await publisher.PublishAsync(message.MessageType, message.Content, cancellationToken);

                if (moduleOptions.DeleteOnPublish)
                {
                    await repository.DeleteMessageAsync(moduleName, message.Id, cancellationToken);
                }
                else
                {
                    await repository.UpdateMessageStatusAsync(
                        moduleName,
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
                _logger.LogError(ex, "Failed to publish outbox message {MessageId} (Type: {MessageType}) for module {ModuleName}", message.Id, message.MessageType, moduleName);

                int nextRetryCount = message.RetryCount + 1;
                string status = nextRetryCount >= moduleOptions.MaxRetryCount ? "Failed" : "Pending";

                await repository.UpdateMessageStatusAsync(
                    moduleName,
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

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

public class OutboxCleanupBackgroundService : BackgroundService
{
    private readonly IServiceProvider _serviceProvider;
    private readonly OutboxOptions _options;
    private readonly ILogger<OutboxCleanupBackgroundService> _logger;

    public OutboxCleanupBackgroundService(
        IServiceProvider serviceProvider,
        IOptions<OutboxOptions> options,
        ILogger<OutboxCleanupBackgroundService> logger)
    {
        _serviceProvider = serviceProvider;
        _options = options.Value;
        _logger = logger;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var modules = _options.Modules;
        if (modules.Count == 0)
        {
            modules = new List<OutboxModuleOptions>
            {
                new OutboxModuleOptions
                {
                    ModuleName = "Default",
                    EnableCleanup = _options.EnableCleanup,
                    CleanupInterval = _options.CleanupInterval,
                    OutboxRetentionPeriod = _options.OutboxRetentionPeriod,
                    InboxRetentionPeriod = _options.InboxRetentionPeriod
                }
            };
        }

        var activeCleanupModules = modules.Where(m => m.EnableCleanup).ToList();
        if (activeCleanupModules.Count == 0)
        {
            _logger.LogInformation("Outbox and Inbox cleanup background service is disabled or has no active modules configured.");
            return;
        }

        _logger.LogInformation("OutboxCleanupBackgroundService started running for {Count} modules.", activeCleanupModules.Count);

        // Run the loop
        while (!stoppingToken.IsCancellationRequested)
        {
            var now = DateTimeOffset.UtcNow;

            foreach (var module in activeCleanupModules)
            {
                try
                {
                    await PerformCleanupAsync(module, stoppingToken);
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Error occurred during background cleanup for module {ModuleName}", module.ModuleName);
                }
            }

            // Determine sleep interval (minimum interval of active modules, default to 1 hour if not specified)
            var minInterval = activeCleanupModules.Min(m => m.CleanupInterval);
            if (minInterval <= TimeSpan.Zero)
            {
                minInterval = TimeSpan.FromHours(1);
            }

            try
            {
                await Task.Delay(minInterval, stoppingToken);
            }
            catch (OperationCanceledException)
            {
                // Service stopping
            }
        }
    }

    private async Task PerformCleanupAsync(OutboxModuleOptions module, CancellationToken cancellationToken)
    {
        var moduleName = module.ModuleName;
        using var scope = _serviceProvider.CreateScope();

        var repository = scope.ServiceProvider.GetKeyedService<IOutboxRepository>(moduleName) 
                         ?? scope.ServiceProvider.GetService<IOutboxRepository>();
        var inboxProcessor = scope.ServiceProvider.GetKeyedService<IInboxProcessor>(moduleName)
                             ?? scope.ServiceProvider.GetService<IInboxProcessor>();

        if (repository == null && inboxProcessor == null)
        {
            _logger.LogWarning("Neither IOutboxRepository nor IInboxProcessor is registered for module {ModuleName}. Skipping cleanup.", moduleName);
            return;
        }

        var now = DateTimeOffset.UtcNow;

        if (repository != null)
        {
            var outboxThreshold = now.Subtract(module.OutboxRetentionPeriod);
            _logger.LogDebug("Running outbox cleanup for module {ModuleName} (older than {Threshold})", moduleName, outboxThreshold);
            await repository.DeleteOldMessagesAsync(moduleName, outboxThreshold, cancellationToken);
        }

        if (inboxProcessor != null)
        {
            var inboxThreshold = now.Subtract(module.InboxRetentionPeriod);
            _logger.LogDebug("Running inbox cleanup for module {ModuleName} (older than {Threshold})", moduleName, inboxThreshold);
            await inboxProcessor.DeleteOldMessagesAsync(moduleName, inboxThreshold, cancellationToken);
        }

        _logger.LogInformation("Successfully completed Outbox and Inbox cleanup for module {ModuleName}", moduleName);
    }
}

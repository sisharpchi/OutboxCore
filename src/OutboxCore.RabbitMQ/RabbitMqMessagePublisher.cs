using System;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Options;
using OutboxCore.Abstractions;
using OutboxCore.RabbitMQ.Configuration;
using RabbitMQ.Client;

namespace OutboxCore.RabbitMQ;

public class RabbitMqMessagePublisher : IMessagePublisher, IDisposable
{
    private readonly RabbitMqOptions _options;
    private IConnection? _connection;
    private IModel? _channel;
    private readonly object _lock = new();

    public RabbitMqMessagePublisher(IOptions<RabbitMqOptions> options)
    {
        _options = options.Value;
    }

    public RabbitMqMessagePublisher(RabbitMqOptions options)
    {
        _options = options;
    }

    private void EnsureConnectionAndChannel()
    {
        if (_channel != null && _channel.IsOpen) return;

        lock (_lock)
        {
            if (_channel != null && _channel.IsOpen) return;

            _channel?.Dispose();
            _connection?.Dispose();

            var factory = new ConnectionFactory
            {
                HostName = _options.HostName,
                Port = _options.Port,
                UserName = _options.UserName,
                Password = _options.Password,
                VirtualHost = _options.VirtualHost,
                DispatchConsumersAsync = true
            };

            _connection = factory.CreateConnection();
            _channel = _connection.CreateModel();

            _channel.ExchangeDeclare(
                exchange: _options.ExchangeName,
                type: _options.ExchangeType,
                durable: true,
                autoDelete: false,
                arguments: null);
        }
    }

    public Task PublishAsync<T>(T message, CancellationToken cancellationToken = default) where T : class
    {
        var messageType = typeof(T).FullName ?? typeof(T).Name;
        var content = JsonSerializer.Serialize(message);
        return PublishAsync(messageType, content, cancellationToken);
    }

    public Task PublishAsync(string messageType, string content, CancellationToken cancellationToken = default)
    {
        EnsureConnectionAndChannel();

        var body = Encoding.UTF8.GetBytes(content);

        var properties = _channel!.CreateBasicProperties();
        properties.Persistent = true;
        properties.Type = messageType;

        lock (_lock)
        {
            _channel.BasicPublish(
                exchange: _options.ExchangeName,
                routingKey: messageType,
                basicProperties: properties,
                body: body);
        }

        return Task.CompletedTask;
    }

    public void Dispose()
    {
        _channel?.Dispose();
        _connection?.Dispose();
    }
}

using System.Collections.Concurrent;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;

namespace OutboxCore.Background;

public class OutboxChannel : IOutboxChannel
{
    private readonly ConcurrentDictionary<string, Channel<byte>> _channels = new();

    private Channel<byte> GetOrCreateChannel(string moduleName)
    {
        return _channels.GetOrAdd(moduleName, _ => Channel.CreateBounded<byte>(new BoundedChannelOptions(1)
        {
            FullMode = BoundedChannelFullMode.DropWrite
        }));
    }

    public ValueTask WriteAsync(string moduleName, CancellationToken cancellationToken = default)
    {
        var channel = GetOrCreateChannel(moduleName);
        return channel.Writer.WriteAsync(0, cancellationToken);
    }

    public ValueTask<bool> WaitToReadAsync(string moduleName, CancellationToken cancellationToken = default)
    {
        var channel = GetOrCreateChannel(moduleName);
        return channel.Reader.WaitToReadAsync(cancellationToken);
    }
}

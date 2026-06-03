using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;

namespace OutboxCore.Background;

public class OutboxChannel : IOutboxChannel
{
    private readonly Channel<byte> _channel = Channel.CreateBounded<byte>(new BoundedChannelOptions(1)
    {
        FullMode = BoundedChannelFullMode.DropWrite
    });

    public ValueTask WriteAsync(CancellationToken cancellationToken = default)
    {
        return _channel.Writer.WriteAsync(0, cancellationToken);
    }

    public ValueTask<bool> WaitToReadAsync(CancellationToken cancellationToken = default)
    {
        return _channel.Reader.WaitToReadAsync(cancellationToken);
    }
}

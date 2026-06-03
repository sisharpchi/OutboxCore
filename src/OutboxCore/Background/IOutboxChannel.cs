using System.Threading;
using System.Threading.Tasks;

namespace OutboxCore.Background;

public interface IOutboxChannel
{
    ValueTask WriteAsync(CancellationToken cancellationToken = default);
    ValueTask<bool> WaitToReadAsync(CancellationToken cancellationToken = default);
}

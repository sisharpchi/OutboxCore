using System.Threading;
using System.Threading.Tasks;

namespace OutboxCore.Background;

public interface IOutboxChannel
{
    ValueTask WriteAsync(string moduleName, CancellationToken cancellationToken = default);
    ValueTask<bool> WaitToReadAsync(string moduleName, CancellationToken cancellationToken = default);
}

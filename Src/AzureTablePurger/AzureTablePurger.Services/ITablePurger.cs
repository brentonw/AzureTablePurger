using System;
using System.Threading;
using System.Threading.Tasks;

namespace AzureTablePurger.Services
{
    public interface ITablePurger
    {
        Task<Tuple<int, int>> PurgeEntitiesAsync(PurgeEntitiesOptions options, CancellationToken cancellationToken);
    }
}
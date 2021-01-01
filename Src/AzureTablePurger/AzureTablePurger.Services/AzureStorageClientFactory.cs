using System.Collections.Concurrent;

using Microsoft.Azure.Cosmos.Table;
using Microsoft.Extensions.Logging;

namespace AzureTablePurger.Services
{
    /// <summary>
    /// Used to create clients for Azure Storage accounts. A single instance of a specific client is created and cached.
    /// </summary>
    public class AzureStorageClientFactory : IAzureStorageClientFactory
    {
        private static readonly ConcurrentDictionary<string, CloudTableClient> CloudTableClientCache = new ConcurrentDictionary<string, CloudTableClient>();

        private readonly ILogger<AzureStorageClientFactory> _logger;
        
        public AzureStorageClientFactory(ILogger<AzureStorageClientFactory> logger)
        {
            _logger = logger;
        }

        public CloudTableClient GetCloudTableClient(string connectionString)
        {
            if (CloudTableClientCache.ContainsKey(connectionString))
            {
                return CloudTableClientCache[connectionString];
            }

            _logger.LogDebug("CloudTableClient not found in cache. Creating new one and adding to cache");

            var account = CloudStorageAccount.Parse(connectionString);
            var newTableClient = account.CreateCloudTableClient();

            bool resultOfAdd = CloudTableClientCache.TryAdd(connectionString, newTableClient);

            if (!resultOfAdd)
            {
                _logger.LogDebug("Adding CloudTableClient to cache failed. Another thread must have beat us to it. Obtaining and returning the one in cache");
                return CloudTableClientCache[connectionString];
            }

            return newTableClient;
        }
    }
}

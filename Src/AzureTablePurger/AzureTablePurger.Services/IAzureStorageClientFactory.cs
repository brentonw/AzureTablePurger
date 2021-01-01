using Microsoft.Azure.Cosmos.Table;

namespace AzureTablePurger.Services
{
    public interface IAzureStorageClientFactory
    {
        CloudTableClient GetCloudTableClient(string connectionString);
    }
}
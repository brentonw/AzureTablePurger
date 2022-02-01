using System;

using Microsoft.Azure.Cosmos.Table;

namespace AzureTablePurger.Services
{
    public interface IPartitionKeyHandler
    {
        TableQuery GetTableQuery(int purgeEntitiesOlderThanDays, string partitionKeyPrefix = "");

        DateTime ConvertPartitionKeyToDateTime(string partitionKey, string partitionKeyPrefix);

        string GetPartitionKeyForDate(DateTime date);

        TableQuery GetTableQuery(string lowerBoundPartitionKey, string upperBoundPartitionKey, string partitionKeyPrefix);
    }
}

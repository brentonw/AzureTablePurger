using System;

using Microsoft.Azure.Cosmos.Table;

namespace AzureTablePurger.Services
{
    public interface IPartitionKeyHandler
    {
        TableQuery GetTableQuery(int purgeEntitiesOlderThanDays);

        DateTime ConvertPartitionKeyToDateTime(string partitionKey);

        string GetPartitionKeyForDate(DateTime date);

        TableQuery GetTableQuery(string lowerBoundPartitionKey, string upperBoundPartitionKey);
    }
}

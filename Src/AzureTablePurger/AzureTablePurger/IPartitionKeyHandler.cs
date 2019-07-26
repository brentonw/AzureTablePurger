using System;
using Microsoft.WindowsAzure.Storage.Table;

namespace AzureTablePurger
{
    public interface IPartitionKeyHandler
    {
        TableQuery GetTableQuery(int purgeEntitiesOlderThanDays);
        DateTime ConvertPartitionKeyToDateTime(string partitionKey);
    }
}
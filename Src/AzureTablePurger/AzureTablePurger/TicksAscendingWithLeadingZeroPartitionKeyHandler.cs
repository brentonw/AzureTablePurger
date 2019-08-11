using System;
using Microsoft.WindowsAzure.Storage.Table;

namespace AzureTablePurger
{
    public class TicksAscendingWithLeadingZeroPartitionKeyHandler : IPartitionKeyHandler
    {
        public TableQuery GetTableQuery(int purgeEntitiesOlderThanDays)
        {
            var maximumPartitionKeyToDelete = GetMaximumPartitionKeyToDelete(purgeEntitiesOlderThanDays);

            var query = new TableQuery()
                .Where(TableQuery.GenerateFilterCondition("PartitionKey", QueryComparisons.LessThanOrEqual, maximumPartitionKeyToDelete))
                .Select(new[] { "PartitionKey", "RowKey" });

            return query;
        }

        public DateTime ConvertPartitionKeyToDateTime(string partitionKey)
        {
            return new DateTime(long.Parse(partitionKey));
        }

        private string GetMaximumPartitionKeyToDelete(int purgeRecordsOlderThanDays)
        {
            return DateTime.UtcNow.AddDays(-1 * purgeRecordsOlderThanDays).Ticks.ToString("D19");
        }
    }
}
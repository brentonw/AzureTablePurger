using System;

using Microsoft.Azure.Cosmos.Table;
using Microsoft.Extensions.Logging;

namespace AzureTablePurger.Services
{
    public class TicksAscendingWithLeadingZeroPartitionKeyHandler : IPartitionKeyHandler
    {
        private readonly ILogger<TicksAscendingWithLeadingZeroPartitionKeyHandler> _logger;

        public TicksAscendingWithLeadingZeroPartitionKeyHandler(ILogger<TicksAscendingWithLeadingZeroPartitionKeyHandler> logger)
        {
            _logger = logger;
        }

        public TableQuery GetTableQuery(int purgeEntitiesOlderThanDays)
        {
            var maximumPartitionKeyToDelete = GetMaximumPartitionKeyToDelete(purgeEntitiesOlderThanDays);

            return GetTableQuery(null, maximumPartitionKeyToDelete);
        }

        public TableQuery GetTableQuery(string lowerBoundPartitionKey, string upperBoundPartitionKey)
        {
            if (string.IsNullOrEmpty(lowerBoundPartitionKey))
            {
                lowerBoundPartitionKey = "0";
            }

            var lowerBoundDateTime = ConvertPartitionKeyToDateTime(lowerBoundPartitionKey);
            var upperBoundDateTime = ConvertPartitionKeyToDateTime(upperBoundPartitionKey);
            _logger.LogDebug($"Generating table query: lowerBound partitionKey={lowerBoundPartitionKey} ({lowerBoundDateTime}), upperBound partitionKey={upperBoundPartitionKey} ({upperBoundDateTime})");

            var lowerBound = TableQuery.GenerateFilterCondition("PartitionKey", QueryComparisons.GreaterThanOrEqual, lowerBoundPartitionKey);
            var upperBound = TableQuery.GenerateFilterCondition("PartitionKey", QueryComparisons.LessThan, upperBoundPartitionKey);
            var combinedFilter = TableQuery.CombineFilters(lowerBound, TableOperators.And, upperBound);

            var query = new TableQuery()
                .Where(combinedFilter)
                .Select(new[] { "PartitionKey", "RowKey" });

            return query;
        }

        public DateTime ConvertPartitionKeyToDateTime(string partitionKey)
        {
            var result = long.TryParse(partitionKey, out long ticks);

            if (!result)
            {
                throw new ArgumentException($"PartitionKey is not in the expected format: {partitionKey}", nameof(partitionKey));
            }

            return new DateTime(ticks);
        }

        public string GetPartitionKeyForDate(DateTime date)
        {
            return date.Ticks.ToString("D19");
        }

        private string GetMaximumPartitionKeyToDelete(int purgeRecordsOlderThanDays)
        {
            return GetPartitionKeyForDate(DateTime.UtcNow.AddDays(-1 * purgeRecordsOlderThanDays));
        }
    }
}
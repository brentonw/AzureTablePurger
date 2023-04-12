using System;
using System.Globalization;
using Microsoft.Azure.Cosmos.Table;
using Microsoft.Extensions.Logging;

namespace AzureTablePurger.Services
{
    public class StringDateRowKeyHandler : IEntityQueryHandler
    {
        private readonly ILogger _logger;

        public StringDateRowKeyHandler(ILogger<StringDateRowKeyHandler> logger)
        {
            _logger = logger;
        }

        public TableQuery GetTableQuery(int purgeEntitiesOlderThanDays)
        {

            var maximumPartitionKeyToDelete = GetMaximumToDelete(purgeEntitiesOlderThanDays);

            _logger.LogDebug($"{nameof(DynamicTableEntity.RowKey)}: {purgeEntitiesOlderThanDays}");

            return GetTableQuery(null, maximumPartitionKeyToDelete);
        }

        public TableQuery GetTableQuery(string lowerBoundPartitionKey, string upperBoundRowKey)
        {

            var upperBound = TableQuery.GenerateFilterCondition(nameof(DynamicTableEntity.RowKey), QueryComparisons.LessThan, upperBoundRowKey);

            var query = new TableQuery()
                .Where(upperBound)
                .Select(new[] { nameof(DynamicTableEntity.PartitionKey), nameof(DynamicTableEntity.RowKey) });


            _logger.LogInformation($"Query : {query.FilterString}");

            return query;
        }

        public string GetKeyForDate(DateTime date)
        {
            return date.ToString("yyyy_MM_dd_HH_mm");
        }

        public DateTime ConvertKeyToDateTime(DynamicTableEntity entry)
        {

            var result = DateTime.ParseExact(entry.RowKey, "yyyy_MM_dd_HH_mm", CultureInfo.InvariantCulture);

            return result;

        }

        private string GetMaximumToDelete(int purgeRecordsOlderThanDays)
        {
            return GetKeyForDate(DateTime.UtcNow.AddDays(-1 * purgeRecordsOlderThanDays));
        }
    }
}
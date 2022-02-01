using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Net;
using System.Threading;
using System.Threading.Tasks;

using AzureTablePurger.Common.Extensions;

using Microsoft.Azure.Cosmos.Table;
using Microsoft.Extensions.Logging;

namespace AzureTablePurger.Services
{
    public class SimpleTablePurger : ITablePurger
    {
        public const int MaxBatchSize = 100;
        public const int ConnectionLimit = 32;

        private readonly IAzureStorageClientFactory _storageClientFactory;
        private readonly ILogger<SimpleTablePurger> _logger;
        private readonly IPartitionKeyHandler _partitionKeyHandler;

        public SimpleTablePurger(IAzureStorageClientFactory storageClientFactory, IPartitionKeyHandler partitionKeyHandler, ILogger<SimpleTablePurger> logger)
        {
            _storageClientFactory = storageClientFactory;
            _partitionKeyHandler = partitionKeyHandler;
            _logger = logger;

            ServicePointManager.DefaultConnectionLimit = ConnectionLimit;
        }

        public async Task<Tuple<int, int>> PurgeEntitiesAsync(PurgeEntitiesOptions options, CancellationToken cancellationToken)
        {
            var sw = new Stopwatch();
            sw.Start();

            _logger.LogInformation($"Starting PurgeEntitiesAsync");

            var tableClient = _storageClientFactory.GetCloudTableClient(options.TargetAccountConnectionString);
            var table = tableClient.GetTableReference(options.TargetTableName);

            _logger.LogInformation($"TargetAccount={tableClient.StorageUri.PrimaryUri}, Table={table.Name}, PurgeRecordsOlderThanDays={options.PurgeRecordsOlderThanDays}");

            var query = _partitionKeyHandler.GetTableQuery(options.PurgeRecordsOlderThanDays, options.PartitionKeyPrefix);
            var continuationToken = new TableContinuationToken();

            int numPagesProcessed = 0;
            int numEntitiesDeleted = 0;

            do
            {
                var page = await table.ExecuteQuerySegmentedAsync(query, continuationToken, cancellationToken);
                var pageNumber = numPagesProcessed + 1;

                if (page.Results.Count == 0)
                {
                    if (numPagesProcessed == 0)
                    {
                        _logger.LogDebug($"No entities were available for purging");
                    }

                    break;
                }

                var firstResultTimestamp = _partitionKeyHandler.ConvertPartitionKeyToDateTime(page.Results.First().PartitionKey, options.PartitionKeyPrefix);
                _logger.LogInformation($"Page {pageNumber}: processing {page.Count()} results starting at timestamp {firstResultTimestamp}");

                var partitionsFromPage = GetPartitionsFromPage(page.Results);

                _logger.LogDebug($"Page {pageNumber}: number of partitions grouped by PartitionKey: {partitionsFromPage.Count}");

                var tasks = new List<Task<int>>();

                foreach (var partition in partitionsFromPage)
                {
                    cancellationToken.ThrowIfCancellationRequested();

                    var chunkedPartition = partition.Chunk(MaxBatchSize).ToList();

                    foreach (var batch in chunkedPartition)
                    {
                        cancellationToken.ThrowIfCancellationRequested();

                        // Implementation 1: one at a time
                        //var recordsDeleted = await DeleteRecordsAsync(table, batch.ToList());
                        //numEntitiesDeleted += recordsDeleted;

                        // Implementation 2: all deletes asynchronously
                        tasks.Add(DeleteRecordsAsync(table, batch.ToList()));
                    }
                }

                // Implementation 2: all deletes asynchronously
                // Wait for and consolidate results
                await Task.WhenAll(tasks);
                var numEntitiesDeletedInThisPage = tasks.Sum(t => t.Result);
                numEntitiesDeleted += numEntitiesDeletedInThisPage;
                _logger.LogDebug($"Page {pageNumber}: processing complete, {numEntitiesDeletedInThisPage} entities deleted");

                continuationToken = page.ContinuationToken;
                numPagesProcessed++;

            } while (continuationToken != null);

            var entitiesPerSecond = numEntitiesDeleted > 0 ? (int)(numEntitiesDeleted / sw.Elapsed.TotalSeconds) : 0;
            var msPerEntity = numEntitiesDeleted > 0 ? (int)(sw.Elapsed.TotalMilliseconds / numEntitiesDeleted) : 0;
            
            _logger.LogInformation($"Finished PurgeEntitiesAsync, processed {numPagesProcessed} pages and deleted {numEntitiesDeleted} entities in {sw.Elapsed} ({entitiesPerSecond} entities per second, or {msPerEntity} ms per entity)");

            return new Tuple<int, int>(numPagesProcessed, numEntitiesDeleted);
        }

        /// <summary>
        /// Executes a batch delete
        /// </summary>
        private async Task<int> DeleteRecordsAsync(CloudTable table, IList<DynamicTableEntity> batch)
        {
            if (batch.Count > MaxBatchSize)
            {
                throw new ArgumentException($"Batch size of {batch.Count} is larger than the maximum allowed size of {MaxBatchSize}");
            }

            var partitionKey = batch.First().PartitionKey;

            if (batch.Any(entity => entity.PartitionKey != partitionKey))
            {
                throw new ArgumentException($"Not all entities in the batch contain the same partitionKey");
            }

            _logger.LogTrace($"Deleting {batch.Count} rows from partitionKey={partitionKey}");

            var batchOperation = new TableBatchOperation();

            foreach (var entity in batch)
            {
                batchOperation.Delete(entity);
            }

            try
            {
                await table.ExecuteBatchAsync(batchOperation);

                return batch.Count;
            }
            catch (StorageException ex)
            {
                if (ex.RequestInformation.HttpStatusCode == 404 &&
                    ex.RequestInformation.ExtendedErrorInformation.ErrorCode == "ResourceNotFound")
                {
                    _logger.LogWarning($"Failed to delete rows from partitionKey={partitionKey}. Data has already been deleted, ex.Message={ex.Message}, HttpStatusCode={ex.RequestInformation.HttpStatusCode}, ErrorCode={ex.RequestInformation.ExtendedErrorInformation.ErrorCode}, ErrorMessage={ex.RequestInformation.ExtendedErrorInformation.ErrorMessage}");
                    return 0;
                }

                _logger.LogError($"Failed to delete rows from partitionKey={partitionKey}. Unknown error. ex.Message={ex.Message}, HttpStatusCode={ex.RequestInformation.HttpStatusCode}, ErrorCode={ex.RequestInformation.ExtendedErrorInformation.ErrorCode}, ErrorMessage={ex.RequestInformation.ExtendedErrorInformation.ErrorMessage}");
                throw;
            }
        }

        /// <summary>
        /// Breaks up a result page into partitions grouped by PartitionKey
        /// </summary>
        private IList<IList<DynamicTableEntity>> GetPartitionsFromPage(IList<DynamicTableEntity> page)
        {
            var result = new List<IList<DynamicTableEntity>>();

            var groupByResult = page.GroupBy(x => x.PartitionKey);

            foreach (var partition in groupByResult.ToList())
            {
                var partitionAsList = partition.ToList();
                result.Add(partitionAsList);
            }

            return result;
        }
    }
}
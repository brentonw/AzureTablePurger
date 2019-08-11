using System.Linq;
using Microsoft.WindowsAzure.Storage.Table;
using Serilog;

namespace AzureTablePurger
{
    /// <summary>
    /// Simple implementation - queries for data then batches up delete operations and executes against the Table Storage API
    /// </summary>
    public class SimpleTablePurger : TablePurgerBase
    {
        public SimpleTablePurger(ILogger logger)
            :base(logger)
        {
        }

        public override void PurgeEntities(out int numEntitiesProcessed, out int numPartitionsProcessed)
        {
            VerifyIsInitialized();

            var query = PartitionKeyHandler.GetTableQuery(PurgeEntitiesOlderThanDays);

            var continuationToken = new TableContinuationToken();

            int partitionCounter = 0;
            int entityCounter = 0;

            do
            {
                var page = TableReference.ExecuteQuerySegmented(query, continuationToken);
                var firstResultTimestamp = PartitionKeyHandler.ConvertPartitionKeyToDateTime(page.Results.First().PartitionKey);

                LogStartingToProcessPage(page, firstResultTimestamp);

                var partitionsFromPage = GetPartitionsFromPage(page.ToList());

                Logger.Information($"Broke into {partitionsFromPage.Count} partitions");

                foreach (var partition in partitionsFromPage)
                {
                    Logger.Verbose($"Processing partition {partitionCounter}");

                    var chunkedPartition = Chunk(partition, MaxBatchSize);

                    foreach (var batch in chunkedPartition)
                    {
                        var batchOperation = new TableBatchOperation();
                        int batchCounter = 0;

                        foreach (var entity in batch)
                        {
                            Logger.Verbose($"Adding entity to batch: PartitionKey={entity.PartitionKey}, RowKey={entity.RowKey}");
                            entityCounter++;

                            batchOperation.Delete(entity);
                            batchCounter++;
                        }

                        Logger.Verbose($"Added {batchCounter} items into batch");
                        Logger.Verbose($"Executing batch delete of {batchCounter} entities");
                        TableReference.ExecuteBatch(batchOperation);

                        ConsoleLogProgressItemProcessed();
                    }

                    partitionCounter++;
                }

                continuationToken = page.ContinuationToken;

            } while (continuationToken != null);

            numPartitionsProcessed = partitionCounter;
            numEntitiesProcessed = entityCounter;
        }
    }
}
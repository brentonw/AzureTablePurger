using System;
using System.Diagnostics;
using System.Linq;
using Microsoft.WindowsAzure.Storage.Table;

namespace AzureTablePurger
{
    class SimpleTablePurger : TablePurgerBase
    {
        public override void PurgeEntities(out int numEntitiesProcessed, out int numPartitionsProcessed)
        {
            VerifyIsInitialized();

            var query = PartitionKeyHandler.GetTableQuery(PurgeEntitiesOlderThanDays);

            var continuationToken = new TableContinuationToken();

            int partitionCounter = 0;
            int entityCounter = 0;

            // Collect and process data
            do
            {
                var page = TableReference.ExecuteQuerySegmented(query, continuationToken);
                var firstResultTimestamp = PartitionKeyHandler.ConvertPartitionKeyToDateTime(page.Results.First().PartitionKey);

                WriteStartingToProcessPage(page, firstResultTimestamp);

                var partitions = GetPartitionsFromPage(page.ToList());

                ConsoleHelper.WriteLineWithColor($"Broke into {partitions.Count} partitions", ConsoleColor.Gray);

                foreach (var partition in partitions)
                {
                    Trace.WriteLine($"Processing partition {partitionCounter}");

                    var batchOperation = new TableBatchOperation();
                    int batchCounter = 0;

                    foreach (var entity in partition)
                    {
                        Trace.WriteLine(
                            $"Adding entity to batch: PartitionKey={entity.PartitionKey}, RowKey={entity.RowKey}");
                        entityCounter++;

                        batchOperation.Delete(entity);
                        batchCounter++;
                    }

                    Trace.WriteLine($"Added {batchCounter} items into batch");

                    partitionCounter++;

                    Trace.WriteLine($"Executing batch delete of {batchCounter} entities");
                    TableReference.ExecuteBatch(batchOperation);

                    WriteProgressItemProcessed();
                }

                continuationToken = page.ContinuationToken;

            } while (continuationToken != null);

            numPartitionsProcessed = partitionCounter;
            numEntitiesProcessed = entityCounter;
        }
    }
}

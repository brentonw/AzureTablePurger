using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.WindowsAzure.Storage.Table;

namespace AzureTablePurger
{
    /// <summary>
    /// Purges entities from an Azure table older than a specified number of days.
    ///
    /// Executes in a parallel using a producer/consumer pattern: one task is querying Azure Table Storage, downloading
    /// data and caching it locally on disk (since there is potentially too much data to deal with in memory, and we produce
    /// a lot faster than we can consume). Another task is picking up items from the in-memory queue in parallel and processing them.
    /// Processing involves reading the cached data from disk, creating batch delete commands and dispatching them to Azure Table Storage.
    /// </summary>
    class ParallelTablePurger : TablePurgerBase
    {
        private const int MaxParallelOperations = 32;
        public const int ConnectionLimit = 32;

        private const string TempDataFileDirectory = "AzureTablePurgerTempData";

        private readonly BlockingCollection<string> _partitionKeyQueue = new BlockingCollection<string>();
        private readonly HashSet<string> _partitionKeysAlreadyQueued = new HashSet<string>();

        private int _globalEntityCounter = 0;
        private int _globalPartitionCounter = 0;

        private CancellationTokenSource _cancellationTokenSource;

        public ParallelTablePurger()
        {
            ServicePointManager.DefaultConnectionLimit = ConnectionLimit;
        }

        public override void PurgeEntities(out int numEntitiesProcessed, out int numPartitionsProcessed)
        {
            void CollectData()
            {
                CollectDataToProcess(PurgeEntitiesOlderThanDays);
            }

            void ProcessData()
            {
                Parallel.ForEach(_partitionKeyQueue.GetConsumingPartitioner(), new ParallelOptions { MaxDegreeOfParallelism = MaxParallelOperations }, ProcessPartition);
            }

            _cancellationTokenSource = new CancellationTokenSource();

            Parallel.Invoke(new ParallelOptions { CancellationToken = _cancellationTokenSource.Token }, CollectData, ProcessData);

            numPartitionsProcessed = _globalPartitionCounter;
            numEntitiesProcessed = _globalEntityCounter;
        }

        
        /// <summary>
        /// Data collection step (ie producer).
        ///
        /// Collects data to process, caches it locally on disk and adds to the _partitionKeyQueue collection for the consumer
        /// </summary>
        private void CollectDataToProcess(int purgeRecordsOlderThanDays)
        {
            var query = PartitionKeyHandler.GetTableQuery(purgeRecordsOlderThanDays);
            var continuationToken = new TableContinuationToken();

            try
            {
                // Collect data
                do
                {
                    var page = TableReference.ExecuteQuerySegmented(query, continuationToken);
                    var firstResultTimestamp = PartitionKeyHandler.ConvertPartitionKeyToDateTime(page.Results.First().PartitionKey);

                    WriteStartingToProcessPage(page, firstResultTimestamp);

                    PartitionPageAndQueueForProcessing(page.Results);

                    continuationToken = page.ContinuationToken;
                    // TODO: temp for testing
                    // continuationToken = null;
                } while (continuationToken != null);

            }
            finally
            {
                _partitionKeyQueue.CompleteAdding();
            }
        }

        /// <summary>
        /// Partitions up a page of data, caches locally to a temp file and adds into 2 in-memory data structures:
        ///
        /// _partitionKeyQueue which is the queue that the consumer pulls from
        ///
        /// _partitionKeysAlreadyQueued keeps track of what we have already queued. We need this to handle situations where
        /// data in a single partition spans multiple pages. After we process a given partition as part of a page of results,
        /// we write the entity keys to a temp file and queue that PartitionKey for processing. It's possible that the next page
        /// of data we get will have more data for the previous partition, so we open the file we just created and write more data
        /// into it. At this point we don't want to re-queue that same PartitionKey for processing, since it's already queued up.
        /// </summary>
        private void PartitionPageAndQueueForProcessing(List<DynamicTableEntity> pageResults)
        {
            _cancellationTokenSource.Token.ThrowIfCancellationRequested();

            var partitionsFromPage = GetPartitionsFromPage(pageResults);

            foreach (var partition in partitionsFromPage)
            {
                var partitionKey = partition.First().PartitionKey;

                using (var streamWriter = GetStreamWriterForPartitionTempFile(partitionKey))
                {
                    foreach (var entity in partition)
                    {
                        var lineToWrite = $"{entity.PartitionKey},{entity.RowKey}";

                        streamWriter.WriteLine(lineToWrite);
                        Interlocked.Increment(ref _globalEntityCounter);
                    }
                }

                if (!_partitionKeysAlreadyQueued.Contains(partitionKey))
                {
                    _partitionKeysAlreadyQueued.Add(partitionKey);
                    _partitionKeyQueue.Add(partitionKey);

                    Interlocked.Increment(ref _globalPartitionCounter);
                    WriteProgressItemQueued();
                }
            }
        }

        /// <summary>
        /// Process a specific partition.
        ///
        /// Reads all entity keys from temp file on disk
        /// </summary>
        private void ProcessPartition(string partitionKeyForPartition)
        {
            try
            {
                var allDataFromFile = GetDataFromPartitionTempFile(partitionKeyForPartition);

                var batchOperation = new TableBatchOperation();

                for (var index = 0; index < allDataFromFile.Length; index++)
                {
                    var line = allDataFromFile[index];
                    var indexOfComma = line.IndexOf(',');
                    var indexOfRowKeyStart = indexOfComma + 1;
                    var partitionKeyForEntity = line.Substring(0, indexOfComma);
                    var rowKeyForEntity = line.Substring(indexOfRowKeyStart, line.Length - indexOfRowKeyStart);

                    var entity = new DynamicTableEntity(partitionKeyForEntity, rowKeyForEntity) { ETag = "*" };

                    batchOperation.Delete(entity);

                    if (index % 100 == 0)
                    {
                        TableReference.ExecuteBatch(batchOperation);
                        batchOperation = new TableBatchOperation();
                    }
                }

                if (batchOperation.Count > 0)
                {
                    TableReference.ExecuteBatch(batchOperation);
                }

                DeletePartitionTempFile(partitionKeyForPartition);
                _partitionKeysAlreadyQueued.Remove(partitionKeyForPartition);

                WriteProgressItemProcessed();
            }
            catch (Exception)
            {
                ConsoleHelper.WriteWithColor($"Error processing partition {partitionKeyForPartition}", ConsoleColor.Red);
                _cancellationTokenSource.Cancel();
                throw;
            }
        }

        private StreamWriter GetStreamWriterForPartitionTempFile(string entityPartitionKey)
        {
            var tempFile = new FileInfo(GetPartitionTempFileFullPath(entityPartitionKey));
            tempFile.Directory.Create();

            return new StreamWriter(File.OpenWrite(tempFile.FullName));
        }

        private string[] GetDataFromPartitionTempFile(string entityPartitionKey)
        {
            return File.ReadAllLines(GetPartitionTempFileFullPath(entityPartitionKey));
        }

        private string GetPartitionTempFileFullPath(string entityPartitionKey)
        {
            return Path.Combine(TempDataFileDirectory, $"{entityPartitionKey}.txt");
        }

        private void DeletePartitionTempFile(string entityPartitionKey)
        {
            File.Delete(GetPartitionTempFileFullPath(entityPartitionKey));
        }
    }
}
